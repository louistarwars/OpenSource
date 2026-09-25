"""Light instrumentation of asyncio synchronisation primitives.

asyncio's ``Lock`` and ``Semaphore`` do not remember *who* holds them, which
is exactly what a deadlock report needs.  We wrap ``acquire``/``release`` so
that, while a :class:`~detangle.SimLoop` is running, the loop records the
holders.  Outside of a simulation the wrappers are pure pass-throughs.

The wrappers are installed once (idempotently) the first time a simulation
loop is created, and they never change asyncio's behaviour.
"""

from __future__ import annotations

import asyncio
import functools
from types import CodeType
from typing import Any

_installed = False

#: code object -> (primitive kind, human readable description)
PRIMITIVE_CODES: dict[CodeType, tuple[str, str]] = {}


def _running_sim_loop() -> Any:
    loop = asyncio.events._get_running_loop()
    if loop is not None and getattr(loop, "_detangle_sim", False):
        return loop
    return None


def _register(func: Any, kind: str, description: str) -> None:
    code = getattr(func, "__code__", None)
    if code is not None:
        PRIMITIVE_CODES[code] = (kind, description)


def _wrap(orig_acquire: Any, orig_release: Any) -> tuple[Any, Any]:
    @functools.wraps(orig_acquire)
    async def acquire(self: Any) -> bool:
        result = await orig_acquire(self)
        loop = _running_sim_loop()
        if loop is not None:
            loop._note_acquire(self)
        return bool(result)

    @functools.wraps(orig_release)
    def release(self: Any) -> None:
        orig_release(self)
        loop = _running_sim_loop()
        if loop is not None:
            loop._note_release(self)

    return acquire, release


def install() -> None:
    global _installed
    if _installed:
        return
    _installed = True

    for cls, kind in ((asyncio.Lock, "lock"), (asyncio.Semaphore, "semaphore")):
        orig_acquire = cls.acquire
        orig_release = cls.release
        _register(orig_acquire, kind, f"acquire {cls.__name__}")
        cls.acquire, cls.release = _wrap(orig_acquire, orig_release)  # type: ignore[method-assign]

    _register(asyncio.Event.wait, "event", "wait for Event to be set")
    _register(asyncio.Condition.wait, "condition", "wait on Condition")
    _register(asyncio.Queue.get, "queue-get", "get an item from Queue")
    _register(asyncio.Queue.put, "queue-put", "put an item into full Queue")
    _register(asyncio.Queue.join, "queue-join", "wait for Queue.join()")
    barrier = getattr(asyncio, "Barrier", None)
    if barrier is not None:
        _register(barrier.wait, "barrier", "wait on Barrier")
    _register(getattr(asyncio.StreamReader, "_wait_for_data", None), "stream", "wait for data")
    _register(asyncio.sleep, "sleep", "sleep")
    _register(asyncio.wait, "wait", "asyncio.wait()")
    _register(getattr(asyncio.tasks, "_wait", None), "wait", "asyncio.wait()")
    _register(asyncio.wait_for, "wait_for", "asyncio.wait_for()")
    task_group = getattr(asyncio, "TaskGroup", None)
    if task_group is not None:
        _register(task_group.__aexit__, "taskgroup", "wait for TaskGroup children")
        aexit = getattr(task_group, "_aexit", None)
        if aexit is not None:
            _register(aexit, "taskgroup", "wait for TaskGroup children")
