"""Helpers to inspect suspended coroutines and locate "user" code."""

from __future__ import annotations

import asyncio
import linecache
import os
import re
import sys
import sysconfig
from collections.abc import Iterator
from types import FrameType, TracebackType
from typing import Any, NamedTuple

_ASYNCIO_DIR = os.path.dirname(asyncio.__file__)
_DETANGLE_DIR = os.path.dirname(os.path.abspath(__file__))
_STDLIB_DIRS = tuple(
    {
        os.path.realpath(p)
        for p in (sysconfig.get_paths().get("stdlib"), sysconfig.get_paths().get("platstdlib"))
        if p
    }
)
_SITE_DIRS = tuple(
    {
        os.path.realpath(p)
        for p in (sysconfig.get_paths().get("purelib"), sysconfig.get_paths().get("platlib"))
        if p
    }
)
_DEFAULT_TASK_NAME = re.compile(r"Task-\d+")


class Location(NamedTuple):
    filename: str
    lineno: int
    function: str

    def short(self) -> str:
        return f"{short_path(self.filename)}:{self.lineno}"

    def source(self) -> str:
        return linecache.getline(self.filename, self.lineno).strip()

    def __str__(self) -> str:
        return f"{self.short()} in {self.function}"


def short_path(filename: str) -> str:
    if filename.startswith("<"):
        return filename
    try:
        rel = os.path.relpath(filename)
    except ValueError:
        rel = filename
    if rel.startswith(".." + os.sep) or os.path.isabs(rel):
        parts = os.path.normpath(filename).split(os.sep)
        return os.sep.join(parts[-2:])
    return rel


_internal_cache: dict[str, bool] = {}


def is_internal(filename: str) -> bool:
    """asyncio, detangle and the rest of the standard library."""
    cached = _internal_cache.get(filename)
    if cached is not None:
        return cached
    real = os.path.realpath(filename) if not filename.startswith("<") else filename
    result = (
        real.startswith((_ASYNCIO_DIR, _DETANGLE_DIR))
        or (real.startswith(_STDLIB_DIRS) and not real.startswith(_SITE_DIRS))
        or filename.startswith("<frozen")
    )
    _internal_cache[filename] = result
    return result


def is_detangle(filename: str) -> bool:
    return os.path.realpath(filename).startswith(_DETANGLE_DIR)


def is_third_party(filename: str) -> bool:
    return os.path.realpath(filename).startswith(_SITE_DIRS)


def frame_location(frame: FrameType) -> Location:
    return Location(frame.f_code.co_filename, frame.f_lineno, frame.f_code.co_name)


def coroutine_frames(coro: Any) -> list[FrameType]:
    """Frames of a suspended coroutine chain, outermost first."""
    frames: list[FrameType] = []
    seen = 0
    while coro is not None and seen < 256:
        seen += 1
        frame = (
            getattr(coro, "cr_frame", None)
            or getattr(coro, "gi_frame", None)
            or getattr(coro, "ag_frame", None)
        )
        if frame is None:
            break
        frames.append(frame)
        coro = (
            getattr(coro, "cr_await", None)
            or getattr(coro, "gi_yieldfrom", None)
            or getattr(coro, "ag_await", None)
        )
    return frames


def best_user_frame(frames: list[FrameType]) -> FrameType | None:
    """Innermost frame in the user's own code (fallback: any non-stdlib frame)."""
    for frame in reversed(frames):
        name = frame.f_code.co_filename
        if not is_internal(name) and not is_third_party(name):
            return frame
    for frame in reversed(frames):
        if not is_internal(frame.f_code.co_filename):
            return frame
    return None


def task_frames(task: asyncio.Task[Any]) -> list[FrameType]:
    try:
        coro = task.get_coro()
    except Exception:  # pragma: no cover - defensive
        return []
    return coroutine_frames(coro)


def traceback_user_location(tb: TracebackType | None) -> Location | None:
    """Innermost user location in a traceback."""
    best: Location | None = None
    fallback: Location | None = None
    while tb is not None:
        loc = Location(tb.tb_frame.f_code.co_filename, tb.tb_lineno, tb.tb_frame.f_code.co_name)
        if not is_internal(loc.filename):
            fallback = loc
            if not is_third_party(loc.filename):
                best = loc
        tb = tb.tb_next
    return best or fallback


def caller_location(depth: int = 1) -> Location | None:
    """First frame outside asyncio/detangle, starting ``depth`` frames up."""
    try:
        frame: FrameType | None = sys._getframe(depth + 1)
    except ValueError:  # pragma: no cover
        return None
    while frame is not None:
        if not is_internal(frame.f_code.co_filename):
            return frame_location(frame)
        frame = frame.f_back
    return None


def iter_tb(tb: TracebackType | None) -> Iterator[TracebackType]:
    while tb is not None:
        yield tb
        tb = tb.tb_next


def coro_name(coro: Any) -> str:
    name = getattr(coro, "__qualname__", None) or getattr(coro, "__name__", None)
    if not name:
        name = type(coro).__name__
    return str(name).split(".<locals>.")[-1]


def is_default_task_name(name: str) -> bool:
    return bool(_DEFAULT_TASK_NAME.fullmatch(name))


def short_repr(value: Any, limit: int = 60) -> str:
    try:
        text = repr(value)
    except Exception:  # pragma: no cover - defensive
        text = f"<{type(value).__name__}>"
    if len(text) > limit:
        text = text[: limit - 3] + "..."
    return text
