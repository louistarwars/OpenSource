"""Explain *why* a simulation got stuck: who waits for what, and who holds it."""

from __future__ import annotations

import asyncio
import linecache
import re
from dataclasses import dataclass, field
from types import FrameType
from typing import TYPE_CHECKING, Any

from ._frames import (
    Location,
    best_user_frame,
    coro_name,
    frame_location,
    is_internal,
    task_frames,
)
from ._instrument import PRIMITIVE_CODES

if TYPE_CHECKING:
    from ._loop import SimLoop

__all__ = ["BlockedTask", "DeadlockReport", "analyze"]


@dataclass
class BlockedTask:
    label: str
    coro: str
    location: Location | None
    kind: str
    waiting_for: str
    blockers: list[str] = field(default_factory=list)


@dataclass
class DeadlockReport:
    blocked: list[BlockedTask]
    cycles: list[list[str]]
    main_label: str

    def signature(self) -> tuple[Any, ...]:
        return (
            "deadlock",
            tuple(sorted({(b.coro, b.kind) for b in self.blocked})),
            bool(self.cycles),
        )

    def render(self) -> str:
        n = len(self.blocked)
        head = (
            f"Deadlock: the main task `{self.main_label}` can never finish. "
            f"{n} task{'s are' if n != 1 else ' is'} blocked and nothing else can run:"
        )
        lines = [head]
        width = max((len(b.label) for b in self.blocked), default=0)
        for b in self.blocked:
            text = f"  {b.label:<{width}}  waits {b.waiting_for}"
            if b.blockers:
                joined = ", ".join(b.blockers)
                if b.kind in ("lock", "semaphore"):
                    text += f", held by {joined}"
                elif b.kind in ("task", "gather", "wait", "wait_for", "taskgroup"):
                    text += f": {joined}"
            lines.append(text)
            if b.location is not None:
                src = b.location.source()
                where = f"{' ' * (width + 4)}at {b.location}"
                if src:
                    where += f": {src}"
                lines.append(where)
        for cycle in self.cycles:
            lines.append("  wait-for cycle: " + " -> ".join([*cycle, cycle[0]]))
        return "\n".join(lines)


def _name_of(obj: Any, frames: list[FrameType]) -> str | None:
    """Find a variable (or ``var.attr``) that refers to *obj* in user frames.

    Names that appear on the line being executed win (``async with second:``
    names the lock ``second`` even if ``right`` refers to it too).
    """
    for frame in reversed(frames):
        try:
            items = list(frame.f_locals.items())
        except Exception:  # pragma: no cover - defensive
            continue
        line = linecache.getline(frame.f_code.co_filename, frame.f_lineno)
        words = set(re.findall(r"[A-Za-z_][A-Za-z_0-9]*", line))
        candidates: list[tuple[bool, str]] = []
        for name, value in items:
            if value is obj:
                candidates.append((name in words, name))
        for name, value in items:
            attrs = getattr(value, "__dict__", None)
            if isinstance(attrs, dict):
                for attr, inner in attrs.items():
                    if inner is obj:
                        candidates.append((name in words and attr in words, f"{name}.{attr}"))
        if candidates:
            on_line = [name for hit, name in candidates if hit]
            return on_line[0] if on_line else candidates[0][1]
    return None


def analyze(loop: SimLoop, main: asyncio.Future[Any]) -> DeadlockReport:
    from ._loop import _is_task

    pending = [t for t in loop.tasks if not t.done()]
    blocked: list[BlockedTask] = []
    edges: dict[str, list[str]] = {}

    for task in pending:
        label = loop.label(task)
        frames = task_frames(task)
        user = best_user_frame(frames)
        location = frame_location(user) if user is not None else None
        kind, description, primitive = "future", "for a future that nobody will ever resolve", None
        prim_frame: FrameType | None = None
        for frame in reversed(frames):
            entry = PRIMITIVE_CODES.get(frame.f_code)
            if entry is not None:
                kind, description = entry
                primitive = frame.f_locals.get("self")
                prim_frame = frame
                break
        blockers: list[asyncio.Task[Any]] = []
        waiter = getattr(task, "_fut_waiter", None)
        user_frames = [f for f in frames if not is_internal(f.f_code.co_filename)]
        name = _name_of(primitive, user_frames) if primitive is not None else None
        noun = type(primitive).__name__ if primitive is not None else ""
        what = f"{noun} `{name}`" if name else noun
        if kind in ("lock", "semaphore") and primitive is not None:
            blockers = loop.holders_of(primitive)
            description = f"to acquire {what}"
        elif kind == "event":
            description = f"for {what} to be set (nothing left can set it)"
        elif kind == "condition":
            description = f"to be notified on {what}"
        elif kind == "queue-get":
            description = f"for an item from {what} (empty, and no producer can run)"
        elif kind == "queue-put":
            description = f"for room in {what} (full, and no consumer can run)"
        elif kind == "queue-join":
            description = f"for all items of {what} to be processed"
        elif kind == "barrier":
            description = f"on {what}"
        elif kind == "mailbox" and primitive is not None:
            description = f"for a message on {primitive!r} (no sender can run)"
        elif kind == "stream":
            description = "for data from a stream (the peer never sends any)"
        elif kind == "taskgroup" and primitive is not None:
            children = getattr(primitive, "_tasks", ())
            blockers = [c for c in children if not c.done()]
            description = "for TaskGroup children"
        elif kind == "wait" and prim_frame is not None:
            fs = prim_frame.f_locals.get("fs") or ()
            blockers = [f for f in fs if _is_task(f) and not f.done()]
            description = "in asyncio.wait()"
        elif kind == "wait_for" and prim_frame is not None:
            inner = prim_frame.f_locals.get("fut")
            if _is_task(inner) and not inner.done():
                blockers = [inner]
            description = "in asyncio.wait_for()"
        elif _is_task(waiter):
            blockers = [waiter]
            kind, description = "task", "for task"
        elif waiter is not None and getattr(waiter, "_children", None) is not None:
            children = waiter._children
            blockers = [c for c in children if _is_task(c) and not c.done()]
            kind, description = "gather", "for gather()"
        blockers.sort(key=lambda t: loop.task_info[t].id if t in loop.task_info else 0)
        blocker_labels = [loop.label(b) for b in blockers]
        edges[label] = blocker_labels
        blocked.append(
            BlockedTask(
                label=label,
                coro=coro_name(task.get_coro()),
                location=location,
                kind=kind,
                waiting_for=description,
                blockers=blocker_labels,
            )
        )

    cycles = _find_cycles(edges)
    main_label = loop.label(main) if _is_task(main) else "the main coroutine"
    return DeadlockReport(blocked=blocked, cycles=cycles, main_label=main_label)


def _find_cycles(edges: dict[str, list[str]], limit: int = 5) -> list[list[str]]:
    """Up to *limit* distinct elementary cycles, found in linear time (one per back edge)."""
    cycles: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    state: dict[str, int] = {}  # 1 = on the DFS stack, 2 = finished

    for root in edges:
        if state.get(root) or len(cycles) >= limit:
            continue
        stack: list[tuple[str, int]] = [(root, 0)]
        path: list[str] = [root]
        state[root] = 1
        while stack:
            node, index = stack[-1]
            successors = edges.get(node, [])
            if index < len(successors):
                stack[-1] = (node, index + 1)
                nxt = successors[index]
                if state.get(nxt) == 1:
                    cycle = path[path.index(nxt) :]
                    start = cycle.index(min(cycle))
                    canonical = tuple(cycle[start:] + cycle[:start])
                    if canonical not in seen and len(cycles) < limit:
                        seen.add(canonical)
                        cycles.append(list(canonical))
                elif not state.get(nxt):
                    state[nxt] = 1
                    stack.append((nxt, 0))
                    path.append(nxt)
            else:
                state[node] = 2
                stack.pop()
                path.pop()
    return cycles
