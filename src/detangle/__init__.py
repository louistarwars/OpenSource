"""detangle -- deterministic simulation testing for Python asyncio.

Run your async code under thousands of different (but perfectly
reproducible) schedules, network conditions and fault patterns; get the
race conditions, deadlocks and cancellation bugs back as minimal, replayable
counterexamples.

Quick start::

    import asyncio, detangle

    @detangle.test
    async def test_counter():
        counter = {"value": 0}

        async def incr():
            current = counter["value"]
            await asyncio.sleep(0)          # e.g. a database round-trip
            counter["value"] = current + 1

        await asyncio.gather(incr(), incr())
        assert counter["value"] == 2        # fails: lost update found & shrunk

See https://github.com/louistarwars/OpenSource for the documentation.
"""

from __future__ import annotations

from . import lin, net, strategies
from ._api import (
    choice,
    flip,
    in_simulation,
    inject_cancellation,
    invariant,
    maybe_timeout,
    network,
    note,
    now,
    randint,
    shuffle,
    spawn,
    uniform,
)
from ._choices import Decision, decode_token, encode_token
from ._config import NetConfig, SimConfig
from ._loop import SimLoop, current_loop
from ._runner import Failure, RunResult, execute
from ._version import __version__
from .errors import (
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
)
from .explore import BugReport, ExploreStats, explore, replay, run, test
from .lin import History
from .strategies import DFS, FIFO, PCT, Portfolio, RandomWalk, Replay, Strategy

__all__ = [
    "DFS",
    "FIFO",
    "PCT",
    "BugFound",
    "BugReport",
    "DeadlockError",
    "Decision",
    "DetangleError",
    "ExploreStats",
    "Failure",
    "History",
    "InjectedTimeout",
    "InvariantViolation",
    "NetConfig",
    "NotLinearizable",
    "Portfolio",
    "RandomWalk",
    "RealIOError",
    "Replay",
    "RunResult",
    "SimConfig",
    "SimLoop",
    "SimulationError",
    "StepLimitExceeded",
    "Strategy",
    "TimeLimitExceeded",
    "__version__",
    "choice",
    "current_loop",
    "decode_token",
    "encode_token",
    "execute",
    "explore",
    "flip",
    "in_simulation",
    "inject_cancellation",
    "invariant",
    "lin",
    "maybe_timeout",
    "net",
    "network",
    "note",
    "now",
    "randint",
    "replay",
    "run",
    "shuffle",
    "spawn",
    "strategies",
    "test",
    "uniform",
]
