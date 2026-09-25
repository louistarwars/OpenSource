"""Exceptions raised by detangle."""

from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .explore import BugReport

__all__ = [
    "BugFound",
    "DeadlockError",
    "DetangleError",
    "InjectedTimeout",
    "InvariantViolation",
    "NotLinearizable",
    "RealIOError",
    "SimulationError",
    "StepLimitExceeded",
    "TimeLimitExceeded",
]


class DetangleError(Exception):
    """Base class for all detangle errors."""


class SimulationError(DetangleError, RuntimeError):
    """The simulation itself cannot proceed (misuse, unsupported feature...)."""


class RealIOError(SimulationError, NotImplementedError):
    """The code under test tried to perform real, non-simulated I/O."""


class DeadlockError(DetangleError):
    """Nothing can run anymore, yet the main coroutine has not finished."""

    def __init__(self, message: str, report: Any = None) -> None:
        super().__init__(message)
        self.report = report


if sys.version_info >= (3, 11):

    class InjectedTimeout(TimeoutError):
        """Raised by :func:`detangle.maybe_timeout` when a timeout was injected."""

else:  # pragma: no cover - asyncio.TimeoutError is not the builtin before 3.11

    class InjectedTimeout(asyncio.TimeoutError, TimeoutError):
        """Raised by :func:`detangle.maybe_timeout` when a timeout was injected."""


class StepLimitExceeded(DetangleError):
    """The run executed more steps than allowed (likely a livelock or busy-wait)."""


class TimeLimitExceeded(DetangleError):
    """Virtual time went past the configured limit (likely a retry storm)."""


class InvariantViolation(DetangleError, AssertionError):
    """A registered :func:`detangle.invariant` did not hold after some step."""

    def __init__(self, name: str, message: str = "") -> None:
        text = f"invariant {name!r} violated"
        if message:
            text += f": {message}"
        super().__init__(text)
        self.name = name


class NotLinearizable(DetangleError, AssertionError):
    """A recorded history has no valid linearization for the given model."""

    def __init__(self, message: str, result: Any = None) -> None:
        super().__init__(message)
        self.result = result


class BugFound(DetangleError, AssertionError):
    """Raised by :func:`detangle.explore` / :func:`detangle.test` on failure.

    ``report`` holds everything: the failure, the minimal schedule, its replay
    token and a readable trace of the interleaving.
    """

    def __init__(self, report: BugReport) -> None:
        super().__init__(report.render())
        self.report = report

    @property
    def token(self) -> str:
        return self.report.token


for _cls in (
    BugFound,
    DeadlockError,
    DetangleError,
    InjectedTimeout,
    InvariantViolation,
    NotLinearizable,
    RealIOError,
    SimulationError,
    StepLimitExceeded,
    TimeLimitExceeded,
):
    _cls.__module__ = "detangle"
del _cls
