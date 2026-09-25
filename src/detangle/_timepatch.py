"""Optional patching of the :mod:`time` module to virtual time.

Enabled with ``SimConfig(patch_time=True)``.  While a simulation step runs,
``time.time()``, ``time.monotonic()``, ``time.perf_counter()`` (and their
``_ns`` variants) return virtual time, so code that measures durations with
them behaves consistently with ``asyncio.sleep``.  The originals are restored
between steps, so pytest, logging etc. are unaffected.

Code that did ``from time import monotonic`` at import time keeps a reference
to the real function and is *not* affected.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._loop import SimLoop

#: Virtual ``time.time()`` starts at this epoch (2025-01-01T00:00:00Z).
EPOCH = 1_735_689_600.0

_NAMES = ("time", "monotonic", "perf_counter", "time_ns", "monotonic_ns", "perf_counter_ns")
_ORIGINALS: dict[str, Any] = {name: getattr(time, name) for name in _NAMES}


class TimePatch:
    def __init__(self, loop: SimLoop) -> None:
        self._loop = loop
        self._depth = 0
        now = loop.time
        self._replacements = {
            "time": lambda: EPOCH + now(),
            "monotonic": now,
            "perf_counter": now,
            "time_ns": lambda: int((EPOCH + now()) * 1e9),
            "monotonic_ns": lambda: int(now() * 1e9),
            "perf_counter_ns": lambda: int(now() * 1e9),
        }

    def enter(self) -> None:
        self._depth += 1
        if self._depth == 1:
            for name, func in self._replacements.items():
                setattr(time, name, func)

    def exit(self) -> None:
        self._depth -= 1
        if self._depth == 0:
            for name, func in _ORIGINALS.items():
                setattr(time, name, func)
