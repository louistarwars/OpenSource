"""Execution traces: a readable account of one interleaving."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ._frames import (
    Location,
    best_user_frame,
    frame_location,
    is_internal,
    short_repr,
    task_frames,
    traceback_user_location,
)

if TYPE_CHECKING:
    from ._loop import Lane, SimLoop, TaskInfo

__all__ = ["Trace", "TraceEvent", "Tracer", "fmt_time"]

_CALLBACK_NAMES = {
    "_set_result_unless_cancelled": "timer fired (sleep)",
    "_release_waiter": "timeout fired",
    "_on_timeout": "asyncio.timeout() expired",
    "_on_completion": "asyncio.wait() bookkeeping",
    "_done_callback": "gather() bookkeeping",
    "_run_until_complete_cb": "main finished",
}


@dataclass
class TraceEvent:
    index: int
    time: float
    actor: str
    verb: str
    text: str = ""
    location: Location | None = None
    source: str = ""
    deviation: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "time": self.time,
            "actor": self.actor,
            "verb": self.verb,
            "text": self.text,
            "location": (
                None
                if self.location is None
                else {
                    "file": self.location.filename,
                    "line": self.location.lineno,
                    "function": self.location.function,
                }
            ),
            "source": self.source,
            "deviation": list(self.deviation),
        }


def fmt_time(t: float) -> str:
    if t == 0:
        return "0"
    if abs(t) < 1:
        ms = t * 1000
        return f"{ms:.3f}".rstrip("0").rstrip(".") + "ms"
    return f"{t:.4f}".rstrip("0").rstrip(".") + "s"


@dataclass
class Trace:
    events: list[TraceEvent]
    actors: list[str]

    def render(self, limit: int = 120) -> str:
        events = self.events
        lines: list[str] = []
        omitted = 0
        if limit and len(events) > limit:
            omitted = len(events) - limit
            events = events[-limit:]
        if not events:
            return "  (no events)"
        tw = max(len(fmt_time(e.time)) for e in events)
        aw = min(28, max(len(e.actor) for e in events))
        if omitted:
            lines.append(f"  ... {omitted} earlier events omitted ...")
        for e in events:
            mark = "*" if e.deviation else " "
            head = f"  {e.index:>4} {fmt_time(e.time):>{tw}}  {e.actor:<{aw}} {mark} {e.verb:<7}"
            body = e.text
            if e.location is not None:
                where = e.location.short()
                body = f"{body}  {where}" if body else where
                if e.source:
                    body += f"  | {e.source}"
            lines.append(f"{head} {body}".rstrip())
            if e.deviation:
                pad = " " * (6 + tw + 2 + aw + 1)
                lines.append(f"{pad}^ ran ahead of {', '.join(e.deviation)}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {"actors": self.actors, "events": [e.to_dict() for e in self.events]}


class Tracer:
    """Collects events while a :class:`~detangle.SimLoop` runs."""

    def __init__(self) -> None:
        self.loop: SimLoop | None = None
        self._raw: list[tuple[Any, ...]] = []
        self._pending_deviation: tuple[Any, ...] = ()

    def attach(self, loop: SimLoop) -> None:
        self.loop = loop

    # -- hooks called by the loop --------------------------------------------

    def _now(self) -> float:
        return self.loop.time() if self.loop is not None else 0.0

    def on_spawn(self, task: asyncio.Task[Any], info: TaskInfo) -> None:
        self._raw.append(("spawn", self._now(), info.parent, task, info.spawned_at))

    def on_deviation(self, skipped: Any) -> None:
        self._pending_deviation = tuple(lane for lane in skipped)

    def before_step(self, lane: Lane, handle: asyncio.Handle) -> None:
        pass

    def after_step(self, lane: Lane, handle: asyncio.Handle) -> None:
        deviation = self._pending_deviation
        self._pending_deviation = ()
        now = self._now()
        if lane.task is not None:
            self._raw.append(("step", now, lane.task, self._describe_task(lane.task), deviation))
        elif lane.kind == "thread":
            self._raw.append(("thread", now, lane.label, deviation))
        else:
            desc = self._describe_callback(handle)
            if desc is None and deviation:
                callback = getattr(handle, "_callback", None)
                name = getattr(callback, "__name__", "")
                desc = _CALLBACK_NAMES.get(name, "internal callback")
            if desc is not None:
                self._raw.append(("callback", now, desc, deviation))

    def on_fault(self, text: str) -> None:
        self._raw.append(("fault", self._now(), text))

    def on_net(self, actor: str, text: str) -> None:
        self._raw.append(("net", self._now(), actor, text))

    def on_note(self, task: asyncio.Task[Any] | None, text: str, where: Location | None) -> None:
        self._raw.append(("note", self._now(), task, text, where))

    def on_unhandled(self, context: dict[str, Any]) -> None:
        exc = context.get("exception")
        text = str(context.get("message") or "unhandled error")
        if exc is not None:
            text += f": {type(exc).__name__}: {exc}"
        self._raw.append(("error", self._now(), text))

    # -- description helpers --------------------------------------------------

    def _describe_task(self, task: asyncio.Task[Any]) -> tuple[str, str, Location | None]:
        if task.done():
            if task.cancelled():
                return ("cancel", "task cancelled", None)
            exc = getattr(task, "_exception", None)
            if exc is not None:
                loc = traceback_user_location(exc.__traceback__)
                lines = str(exc).strip().splitlines()
                first = lines[0] if lines else ""
                if len(first) > 70:
                    first = first[:67] + "..."
                text = f"{type(exc).__name__}: {first}" if first else type(exc).__name__
                return ("raise", text, loc)
            result = getattr(task, "_result", None)
            return ("return", "" if result is None else short_repr(result, 50), None)
        frames = task_frames(task)
        user = best_user_frame(frames)
        waiting = ""
        start = frames.index(user) + 1 if user is not None and user in frames else 0
        for frame in frames[start:]:
            if not is_internal(frame.f_code.co_filename):
                continue
            qual = getattr(frame.f_code, "co_qualname", frame.f_code.co_name)
            if frame.f_code.co_name.startswith("__") or qual.startswith("_"):
                continue
            waiting = qual
            break
        loc = frame_location(user) if user is not None else None
        return ("await", waiting, loc)

    def _describe_callback(self, handle: asyncio.Handle) -> str | None:
        callback = getattr(handle, "_callback", None)
        if callback is None:
            return None
        name = getattr(callback, "__name__", "")
        func = getattr(callback, "__func__", callback)
        code = getattr(func, "__code__", None)
        if code is None or is_internal(code.co_filename):
            return None
        qual = getattr(callback, "__qualname__", name) or repr(callback)
        return f"callback {qual}()"

    # -- finalisation ---------------------------------------------------------

    def build(self) -> Trace:
        loop = self.loop
        assert loop is not None

        def lab(x: Any) -> str:
            if x is None:
                return "-"
            if isinstance(x, str):
                return x
            if hasattr(x, "get_coro"):
                return loop.label(x)
            return loop.lane_label(x)

        events: list[TraceEvent] = []
        actors: list[str] = []
        seen: set[str] = set()

        def add(event: TraceEvent) -> None:
            if event.actor not in seen:
                seen.add(event.actor)
                actors.append(event.actor)
            events.append(event)

        for raw in self._raw:
            kind = raw[0]
            idx = len(events) + 1
            if kind == "spawn":
                _, t, parent, task, where = raw
                if parent is None:
                    continue
                add(
                    TraceEvent(
                        idx,
                        t,
                        lab(parent) if parent is not None else "loop",
                        "spawn",
                        lab(task),
                        where,
                        where.source() if where else "",
                    )
                )
            elif kind == "step":
                _, t, task, (verb, text, loc), deviation = raw
                add(
                    TraceEvent(
                        idx,
                        t,
                        lab(task),
                        verb,
                        text,
                        loc,
                        loc.source() if loc else "",
                        tuple(lab(d) for d in deviation),
                    )
                )
            elif kind == "thread":
                _, t, label, deviation = raw
                add(
                    TraceEvent(
                        idx,
                        t,
                        label,
                        "thread",
                        "job ran",
                        None,
                        "",
                        tuple(lab(d) for d in deviation),
                    )
                )
            elif kind == "callback":
                _, t, desc, deviation = raw
                add(
                    TraceEvent(
                        idx, t, "callbacks", "run", desc, None, "", tuple(lab(d) for d in deviation)
                    )
                )
            elif kind == "fault":
                _, t, text = raw
                add(TraceEvent(idx, t, "detangle", "fault", text))
            elif kind == "net":
                _, t, actor, text = raw
                add(TraceEvent(idx, t, actor, "net", text))
            elif kind == "note":
                _, t, task, text, where = raw
                add(
                    TraceEvent(
                        idx, t, lab(task) if task is not None else "note", "note", text, where, ""
                    )
                )
            elif kind == "error":
                _, t, text = raw
                add(TraceEvent(idx, t, "loop", "error", text))
        return Trace(events, actors)
