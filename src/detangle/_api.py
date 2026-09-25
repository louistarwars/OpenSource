"""Helpers meant to be called from code running *inside* a simulation."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine, MutableSequence, Sequence
from typing import Any, TypeVar

from ._choices import DATA
from ._context import current_host
from ._frames import caller_location
from ._loop import SimLoop, current_loop
from .errors import InjectedTimeout

__all__ = [
    "choice",
    "flip",
    "in_simulation",
    "inject_cancellation",
    "invariant",
    "maybe_timeout",
    "network",
    "note",
    "now",
    "randint",
    "shuffle",
    "spawn",
    "uniform",
]

_T = TypeVar("_T")


def in_simulation() -> bool:
    """True when called from code running inside a detangle simulation."""
    return isinstance(asyncio.events._get_running_loop(), SimLoop)


def now() -> float:
    """Current virtual time, in seconds (same as ``loop.time()``)."""
    return current_loop().time()


def choice(options: Sequence[_T]) -> _T:
    """Pick one of *options*.  Explored by the strategy; shrinks to the first one."""
    if not options:
        raise IndexError("cannot choose from an empty sequence")
    return options[current_loop().choose(len(options), DATA)]


def randint(a: int, b: int) -> int:
    """An integer in ``[a, b]``.  Explored by the strategy; shrinks towards *a*."""
    if b < a:
        raise ValueError(f"empty range for randint({a}, {b})")
    return a + current_loop().choose(b - a + 1, DATA)


def uniform(a: float, b: float, steps: int = 64) -> float:
    """A float in ``[a, b]`` (one of *steps* evenly spaced values; shrinks towards *a*)."""
    if steps < 2:
        return a
    k = current_loop().choose(steps, DATA)
    return a + (b - a) * k / (steps - 1)


def flip(p: float = 0.5) -> bool:
    """True with probability *p*.  Shrinks towards False."""
    return current_loop().flip(p, DATA)


def shuffle(items: MutableSequence[Any]) -> None:
    """Shuffle *items* in place.  The default (all zeros) is the identity."""
    loop = current_loop()
    for i in range(len(items) - 1, 0, -1):
        j = i - loop.choose(i + 1, DATA)
        items[i], items[j] = items[j], items[i]


def invariant(check: Callable[[], object], name: str | None = None) -> Callable[[], object]:
    """Check ``check()`` after *every* step of the simulation.

    The check fails if it raises ``AssertionError`` (or any exception) or
    returns ``False``.  Returns *check* so it can be used as a decorator.
    """
    current_loop().add_invariant(check, name)
    return check


def note(message: str) -> None:
    """Add a line to the trace shown when a bug is found (no-op otherwise)."""
    loop = current_loop()
    if loop.tracer is not None:
        loop.tracer.on_note(asyncio.current_task(), message, caller_location(1))


def inject_cancellation(task: asyncio.Task[Any], points: int = 8) -> None:
    """Allow the strategy to cancel *task* at one of its next *points* awaits.

    Models timeouts and client disconnects hitting at the worst moment.
    """
    current_loop().mark_interruptible(task, points)


async def maybe_timeout(aw: Awaitable[_T], points: int = 8) -> _T:
    """Await *aw*, but let the strategy "time it out" at any of its awaits.

    Equivalent to ``asyncio.wait_for(aw, timeout=<adversarial>)``: returns the
    result, or cancels the operation at one of its first *points* suspension
    points and raises :class:`TimeoutError`.  Use it to find code that is not
    cancellation-safe (leaked locks, connections, half-applied updates...).
    """
    loop = current_loop()
    task = asyncio.ensure_future(aw)
    loop.mark_interruptible(task, points)
    try:
        return await task
    except asyncio.CancelledError:
        if task in loop.injected and task.cancelled():
            current = asyncio.current_task()
            if current is None or not getattr(current, "cancelling", lambda: 0)():
                raise InjectedTimeout(
                    f"detangle: injected timeout (cancelled at {loop.injected[task]})"
                ) from None
        raise


def spawn(
    coro: Coroutine[Any, Any, _T], *, host: str | None = None, name: str | None = None
) -> asyncio.Task[_T]:
    """Create a task, optionally running on simulated *host* (see :mod:`detangle.net`)."""
    loop = current_loop()
    if host is None:
        return loop.create_task(coro, name=name)
    loop.net.ensure_host(host)
    token = current_host.set(host)
    try:
        return loop.create_task(coro, name=name)
    finally:
        current_host.reset(token)


def network() -> Any:
    """The simulated network (:class:`detangle.net.SimNetwork`) of this run."""
    return current_loop().net
