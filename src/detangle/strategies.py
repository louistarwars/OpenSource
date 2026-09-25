"""Exploration strategies.

A strategy answers the questions a simulation asks while it runs:

* :meth:`Strategy.schedule` -- several lanes (tasks) are ready, which runs next?
* :meth:`Strategy.choose` -- pick an integer in ``range(n)`` (timer jitter,
  network latency, user-level choices...).
* :meth:`Strategy.flip` -- should a fault with probability ``p`` happen?

For every question the answer ``0``/``False`` is asyncio's default behaviour.
Strategies are stateful across runs: :meth:`start` is called before each run
and :meth:`finish` after it, with the decisions that were actually taken.
"""

from __future__ import annotations

import os
import random
from collections.abc import Sequence
from typing import Protocol

from ._choices import SCHED, Decision

__all__ = [
    "DFS",
    "FIFO",
    "PCT",
    "Portfolio",
    "RandomWalk",
    "Replay",
    "Strategy",
    "make_strategy",
]


class LaneLike(Protocol):
    """What strategies can see about a runnable lane."""

    @property
    def id(self) -> int: ...


class Strategy:
    """Base class. The base implementation always picks asyncio's default."""

    name = "strategy"

    def start(self, run_index: int) -> None:
        """Called before every run."""

    def schedule(self, lanes: Sequence[LaneLike]) -> int:
        """Return the index (into *lanes*, oldest first) of the lane to run."""
        return 0

    def choose(self, n: int, kind: str) -> int:
        """Return an integer in ``range(n)``; ``0`` is the default behaviour."""
        return 0

    def flip(self, p: float, kind: str) -> bool:
        """Return True with probability ``p`` (``False`` is the default)."""
        return False

    def finish(self, decisions: Sequence[Decision], failed: bool) -> None:
        """Called after every run with the decisions that were taken."""

    @property
    def exhausted(self) -> bool:
        """True when the strategy has no new schedule to offer."""
        return False

    def describe(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.describe()}>"


class FIFO(Strategy):
    """Exactly asyncio's own ordering. Useful for plain virtual-time runs."""

    name = "fifo"

    def __init__(self) -> None:
        self._runs = 0

    def start(self, run_index: int) -> None:
        self._runs += 1

    @property
    def exhausted(self) -> bool:
        return self._runs >= 1


def _derive_seed(seed: int, *parts: object) -> random.Random:
    return random.Random(":".join(str(p) for p in (seed, *parts)))


def fresh_seed() -> int:
    return int.from_bytes(os.urandom(6), "big")


class RandomWalk(Strategy):
    """Every decision is taken uniformly at random.

    Cheap and surprisingly effective, but it tends to produce "noisy"
    schedules with many context switches; shrinking cleans them up.
    """

    name = "random"

    def __init__(self, seed: int | None = None) -> None:
        self.seed = fresh_seed() if seed is None else seed
        self._rng = _derive_seed(self.seed, 0)

    def start(self, run_index: int) -> None:
        self._rng = _derive_seed(self.seed, "random", run_index)

    def schedule(self, lanes: Sequence[LaneLike]) -> int:
        return self._rng.randrange(len(lanes))

    def choose(self, n: int, kind: str) -> int:
        return self._rng.randrange(n)

    def flip(self, p: float, kind: str) -> bool:
        return self._rng.random() < p

    def describe(self) -> str:
        return f"random(seed={self.seed})"


class PCT(Strategy):
    """Probabilistic Concurrency Testing (Burckhardt et al., ASPLOS 2010).

    Every lane receives a random priority and the highest-priority runnable
    lane always runs.  At ``depth - 1`` randomly chosen steps the running lane
    is demoted below everybody else.  For a bug that needs ``d`` ordering
    constraints to manifest, PCT finds it with probability at least
    ``1 / (n * k**(d-1))`` per run (``n`` lanes, ``k`` steps) -- far better
    than a random walk for "deep" bugs.
    """

    name = "pct"

    def __init__(self, seed: int | None = None, depth: int = 3, horizon: int = 64) -> None:
        if depth < 1:
            raise ValueError("PCT depth must be >= 1")
        self.seed = fresh_seed() if seed is None else seed
        self.depth = depth
        self._horizon = max(2, horizon)
        self._observed = 0
        self._rng = _derive_seed(self.seed, 0)
        self._priorities: dict[int, float] = {}
        self._change_points: dict[int, int] = {}
        self._step = 0

    def start(self, run_index: int) -> None:
        self._rng = _derive_seed(self.seed, "pct", self.depth, run_index)
        self._priorities = {}
        self._step = 0
        k = self._horizon
        points = self._rng.sample(range(1, k + 1), min(self.depth - 1, k))
        # Demotions go *below* every initial priority, earlier ones lower.
        self._change_points = {step: i for i, step in enumerate(sorted(points))}

    def schedule(self, lanes: Sequence[LaneLike]) -> int:
        self._step += 1
        prio = self._priorities
        best = 0
        best_p = -1.0
        for i, lane in enumerate(lanes):
            p = prio.get(lane.id)
            if p is None:
                p = prio[lane.id] = self.depth + self._rng.random()
            if p > best_p:
                best, best_p = i, p
        change = self._change_points.get(self._step)
        if change is not None:
            prio[lanes[best].id] = (change + self._rng.random() * 0.5) / self.depth
        return best

    def choose(self, n: int, kind: str) -> int:
        return self._rng.randrange(n)

    def flip(self, p: float, kind: str) -> bool:
        return self._rng.random() < p

    def finish(self, decisions: Sequence[Decision], failed: bool) -> None:
        # k in the PCT bound: the number of scheduling decisions of a run.
        steps = sum(1 for d in decisions if d.kind == SCHED)
        self._observed = max(self._observed, steps)
        self._horizon = max(2, self._observed)

    def describe(self) -> str:
        return f"pct(depth={self.depth}, seed={self.seed})"


class DFS(Strategy):
    """Exhaustive, bounded, systematic exploration (stateless model checking).

    Explores *every* choice sequence whose total deviation from asyncio's
    default (the sum of the chosen indices, i.e. "delays" in the sense of
    delay-bounded scheduling, Emmi et al. POPL 2011) is at most
    ``max_delays``.  When it finishes without finding a bug you get a real
    guarantee: no bug is reachable within that bound.

    Decisions with more than ``max_branch`` alternatives (e.g. the random
    seed, or ``detangle.uniform``) are not enumerated; they stay at 0.
    """

    name = "dfs"

    def __init__(self, max_delays: int = 2, max_branch: int = 16) -> None:
        if max_delays < 0:
            raise ValueError("max_delays must be >= 0")
        self.max_delays = max_delays
        self.max_branch = max_branch
        self._prefix: list[int] = []
        self._pos = 0
        self._exhausted = False
        self.schedules = 0

    def _next(self, n: int) -> int:
        pos = self._pos
        self._pos += 1
        if pos < len(self._prefix):
            return min(self._prefix[pos], n - 1)
        return 0

    def start(self, run_index: int) -> None:
        self._pos = 0

    def schedule(self, lanes: Sequence[LaneLike]) -> int:
        return self._next(len(lanes))

    def choose(self, n: int, kind: str) -> int:
        return self._next(n)

    def flip(self, p: float, kind: str) -> bool:
        return bool(self._next(2))

    def finish(self, decisions: Sequence[Decision], failed: bool) -> None:
        self.schedules += 1
        costs = [0]
        for d in decisions:
            costs.append(costs[-1] + (d.value if d.n <= self.max_branch else 0))
        for i in range(len(decisions) - 1, -1, -1):
            d = decisions[i]
            if d.n > self.max_branch:
                continue
            if d.value + 1 < d.n and costs[i] + d.value + 1 <= self.max_delays:
                self._prefix = [x.value for x in decisions[:i]] + [d.value + 1]
                return
        self._exhausted = True

    @property
    def exhausted(self) -> bool:
        return self._exhausted

    def describe(self) -> str:
        return f"dfs(max_delays={self.max_delays})"


class Replay(Strategy):
    """Replays a recorded choice sequence; past its end, picks defaults."""

    name = "replay"

    def __init__(self, values: Sequence[int]) -> None:
        self.values = list(values)
        self._pos = 0
        self._runs = 0

    def _next(self, n: int) -> int:
        pos = self._pos
        self._pos += 1
        if pos < len(self.values):
            return min(self.values[pos], n - 1)
        return 0

    def start(self, run_index: int) -> None:
        self._pos = 0
        self._runs += 1

    def schedule(self, lanes: Sequence[LaneLike]) -> int:
        return self._next(len(lanes))

    def choose(self, n: int, kind: str) -> int:
        return self._next(n)

    def flip(self, p: float, kind: str) -> bool:
        return bool(self._next(2))

    @property
    def exhausted(self) -> bool:
        return self._runs >= 1

    def describe(self) -> str:
        return "replay"


class Portfolio(Strategy):
    """Round-robin over several strategies (one per run)."""

    name = "portfolio"

    def __init__(self, strategies: Sequence[Strategy]) -> None:
        if not strategies:
            raise ValueError("Portfolio needs at least one strategy")
        self.strategies = list(strategies)
        self._current = self.strategies[0]
        self._counts = [0] * len(self.strategies)

    def start(self, run_index: int) -> None:
        live = [i for i, s in enumerate(self.strategies) if not s.exhausted]
        idx = live[run_index % len(live)] if live else 0
        self._current = self.strategies[idx]
        self._current.start(self._counts[idx])
        self._counts[idx] += 1

    def schedule(self, lanes: Sequence[LaneLike]) -> int:
        return self._current.schedule(lanes)

    def choose(self, n: int, kind: str) -> int:
        return self._current.choose(n, kind)

    def flip(self, p: float, kind: str) -> bool:
        return self._current.flip(p, kind)

    def finish(self, decisions: Sequence[Decision], failed: bool) -> None:
        self._current.finish(decisions, failed)

    @property
    def exhausted(self) -> bool:
        return all(s.exhausted for s in self.strategies)

    @property
    def current(self) -> Strategy:
        return self._current

    def describe(self) -> str:
        return "auto[" + ", ".join(s.describe() for s in self.strategies) + "]"


def make_strategy(spec: str | Strategy | None, seed: int | None = None) -> Strategy:
    """Build a strategy from a name (``auto``, ``pct``, ``random``, ``dfs``, ``fifo``)."""
    if isinstance(spec, Strategy):
        return spec
    spec = (spec or "auto").strip().lower()
    base = fresh_seed() if seed is None else seed
    name, _, arg = spec.partition(":")
    if name == "auto":
        return Portfolio(
            [
                FIFO(),  # asyncio's own order first: the most "natural" schedule
                PCT(base, depth=2),
                RandomWalk(base),
                PCT(base, depth=3),
                PCT(base, depth=1),
            ]
        )
    if name == "pct":
        return PCT(base, depth=int(arg) if arg else 3)
    if name == "random":
        return RandomWalk(base)
    if name == "dfs":
        return DFS(max_delays=int(arg) if arg else 2)
    if name == "fifo":
        return FIFO()
    raise ValueError(
        f"unknown strategy {spec!r} (expected auto, pct[:depth], random, dfs[:delays], fifo)"
    )
