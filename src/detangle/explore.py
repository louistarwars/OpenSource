"""High-level entry points: run, explore, replay and the ``@test`` decorator."""

from __future__ import annotations

import functools
import inspect
import os
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar, overload

from . import _settings
from ._choices import complexity, decode_token, encode_token, strip_trailing_zeros
from ._config import SimConfig, build_config
from ._database import ExampleDatabase
from ._frames import short_path
from ._runner import Failure, RunResult, execute
from ._shrink import shrink as _shrink
from ._trace import Trace
from .errors import BugFound, DetangleError, NotLinearizable
from .strategies import FIFO, Portfolio, RandomWalk, Replay, Strategy, make_strategy

__all__ = ["BugReport", "ExploreStats", "explore", "replay", "run", "test"]

_T = TypeVar("_T")


@dataclass
class ExploreStats:
    """Summary of an exploration that found no bug."""

    name: str
    runs: int = 0
    distinct_schedules: int = 0
    total_steps: int = 0
    max_steps: int = 0
    max_virtual_time: float = 0.0
    elapsed: float = 0.0
    exhausted: bool = False
    strategy: str = ""
    seed: int | None = None
    replayed_examples: int = 0
    _schedules: set[int] = field(default_factory=set, repr=False)

    def record(self, result: RunResult) -> None:
        self.runs += 1
        self._schedules.add(result.schedule_key())
        self.distinct_schedules = len(self._schedules)
        self.total_steps += result.steps
        self.max_steps = max(self.max_steps, result.steps)
        self.max_virtual_time = max(self.max_virtual_time, result.virtual_time)

    def summary(self) -> str:
        what = "all schedules within the bound" if self.exhausted else "no bug found"
        return (
            f"detangle: {self.name}: {self.runs} runs, {self.distinct_schedules} distinct "
            f"interleavings, {self.total_steps} steps in {self.elapsed:.2f}s -- {what} "
            f"[{self.strategy}]"
        )


@dataclass
class BugReport:
    """Everything detangle knows about a bug it found."""

    name: str
    failure: Failure
    token: str
    values: list[int]
    runs: int
    strategy: str
    seed: int | None
    trace: Trace | None
    deviations: int
    shrink_attempts: int = 0
    original_deviations: int | None = None
    reproduced: bool = True
    secondary: list[Failure] = field(default_factory=list)
    html_report: str | None = None
    source: str = "exploration"

    def reproduce_hint(self) -> list[str]:
        hints = []
        nodeid = os.environ.get("PYTEST_CURRENT_TEST", "").rsplit(" (", 1)[0].strip()
        if nodeid:
            hints.append(f'DETANGLE_REPLAY={self.token} pytest "{nodeid}"')
        hints.append(f'detangle.replay({self.name.rsplit(".", 1)[-1]}, "{self.token}")')
        return hints

    def render(self, trace_limit: int = 80) -> str:
        f = self.failure
        lines: list[str] = []
        where = (
            "a saved example"
            if self.source == "database"
            else f"{self.runs} run{'s' if self.runs != 1 else ''}"
        )
        if self.source == "replay":
            lines.append(f"detangle: replaying {self.token} reproduces a bug in {self.name}")
        else:
            lines.append(f"detangle found a bug in {self.name} after {where} [{self.strategy}]")
        lines.append("")
        if f.kind == "deadlock" and f.deadlock is not None:
            lines.extend("  " + line for line in f.deadlock.render().splitlines())
        else:
            prefix = {
                "unobserved-exception": "unobserved exception",
                "callback-error": "exception in callback",
                "invariant": "invariant violated",
                "step-limit": "step limit exceeded",
                "time-limit": "virtual time limit exceeded",
                "task-leak": "task leak",
            }.get(f.kind)
            headline = f.headline() if f.kind != "unobserved-exception" else f.message
            if f.kind == "invariant" and f.exception is not None:
                headline = str(f.exception)
            lines.append(
                f"  {headline}" if not prefix or prefix in headline else f"  [{prefix}] {headline}"
            )
            if f.location is not None:
                src = f.location.source()
                lines.append(f"    at {f.location}" + (f": {src}" if src else ""))
            if isinstance(f.exception, NotLinearizable):
                lines.append("")
                lines.extend("  " + line for line in str(f.exception).splitlines()[1:])
        for extra in self.secondary[:3]:
            lines.append(f"  also: {extra.message}")
        lines.append("")
        if self.deviations == 0:
            desc = "no deviation from asyncio's default order: this fails on a stock event loop too"
        else:
            s = "s" if self.deviations != 1 else ""
            desc = f"{self.deviations} deviation{s} from asyncio's default behaviour"
        shrunk = ""
        if self.original_deviations is not None and self.original_deviations != self.deviations:
            shrunk = f" (shrunk from {self.original_deviations} in {self.shrink_attempts} replays)"
        lines.append(f"Minimal failing schedule: {desc}{shrunk}.")
        if not self.reproduced:
            lines.append(
                "WARNING: replaying this schedule did not reproduce the failure identically; the "
                "code under test may be nondeterministic (real time, real I/O, threads, "
                "id()/hash() ordering...)."
            )
        if self.trace is not None:
            lines.append("")
            lines.append(
                "Interleaving (* = the scheduler deviated from asyncio's FIFO order here):"
            )
            lines.append(self.trace.render(limit=trace_limit))
        lines.append("")
        lines.append("Reproduce this exact run:")
        for hint in self.reproduce_hint():
            lines.append(f"    {hint}")
        if self.html_report:
            lines.append(f"Interactive report: {self.html_report}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "token": self.token,
            "kind": self.failure.kind,
            "headline": self.failure.headline(),
            "message": self.failure.message,
            "location": str(self.failure.location) if self.failure.location else None,
            "deadlock": self.failure.deadlock.render() if self.failure.deadlock else None,
            "runs": self.runs,
            "strategy": self.strategy,
            "seed": self.seed,
            "deviations": self.deviations,
            "original_deviations": self.original_deviations,
            "reproduced": self.reproduced,
            "trace": self.trace.to_dict() if self.trace else None,
        }


def _deviations(values: list[int]) -> int:
    return complexity(values)[0]


def _count_non_seed(result: RunResult) -> int:
    return sum(1 for d in result.decisions if d.value and d.kind != "seed")


def _resolve(fn: Any) -> Any:
    return getattr(fn, "_detangle_inner", fn)


def _name_of(fn: Any) -> str:
    fn = _resolve(fn)
    target = fn.func if isinstance(fn, functools.partial) else fn
    module = getattr(target, "__module__", None) or ""
    qual = (
        getattr(target, "__qualname__", None) or getattr(target, "__name__", None) or repr(target)
    )
    return f"{module}.{qual}" if module and module != "__main__" else str(qual)


class _Runner:
    """Runs a function under various strategies with a fixed configuration."""

    def __init__(self, fn: Callable[[], Any], config: SimConfig) -> None:
        self.fn = fn
        self.config = config
        self.runs = 0

    def run(self, strategy: Strategy, index: int, *, trace: bool = False) -> RunResult:
        strategy.start(index)
        result = execute(self.fn, config=self.config, strategy=strategy, trace=trace)
        strategy.finish(result.decisions, result.failure is not None)
        self.runs += 1
        return result

    def replay(self, values: list[int], *, trace: bool = False) -> RunResult:
        return self.run(Replay(values), 0, trace=trace)


def _write_html(report: BugReport, report_dir: str) -> str | None:
    try:
        from ._html import render_html

        directory = Path(report_dir)
        directory.mkdir(parents=True, exist_ok=True)
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in report.name)[-80:]
        path = directory / f"{safe}.html"
        path.write_text(render_html(report), encoding="utf-8")
        return short_path(str(path))
    except OSError:
        return None


def _finalize(
    runner: _Runner,
    name: str,
    failing: RunResult,
    *,
    runs: int,
    strategy_desc: str,
    seed: int | None,
    do_shrink: bool,
    source: str,
    report_dir: str | None,
) -> BugReport:
    assert failing.failure is not None
    signature = failing.failure.signature()
    values = strip_trailing_zeros(failing.values)
    original = _count_non_seed(failing)
    attempts = 0
    if do_shrink and values:
        shrunk = _shrink(runner.replay, values, signature)
        values = shrunk.values
        attempts = shrunk.attempts
    final = runner.replay(values, trace=True)
    reproduced = final.failure is not None and final.failure.signature() == signature
    if not reproduced:
        # Nondeterminism: fall back to what we saw originally.
        final_failure = failing.failure
        trace = final.trace if final.failure is not None else None
        secondary = failing.secondary
    else:
        assert final.failure is not None
        final_failure = final.failure
        trace = final.trace
        secondary = final.secondary
    report = BugReport(
        name=name,
        failure=final_failure,
        token=encode_token(values),
        values=values,
        runs=runs,
        strategy=strategy_desc,
        seed=seed,
        trace=trace,
        deviations=_count_non_seed(final) if reproduced else _deviations(values),
        shrink_attempts=attempts,
        original_deviations=original if do_shrink else None,
        reproduced=reproduced,
        secondary=secondary,
        source=source,
    )
    if report_dir:
        report.html_report = _write_html(report, report_dir)
    return report


def _raise(report: BugReport) -> None:
    __tracebackhide__ = True
    exc = BugFound(report)
    cause = report.failure.exception
    if cause is not None and not isinstance(cause, BugFound):
        raise exc from cause
    raise exc from None


def explore(
    fn: Callable[[], Coroutine[Any, Any, Any]],
    *,
    runs: int | None = None,
    strategy: str | Strategy | None = None,
    seed: int | None = None,
    max_duration: float | None = None,
    shrink: bool | None = None,
    config: SimConfig | None = None,
    database: str | bool | None = None,
    report_dir: str | None = None,
    replay: str | None = None,
    name: str | None = None,
    **options: Any,
) -> ExploreStats:
    """Run *fn* (an ``async def`` taking no arguments) under many schedules.

    Returns :class:`ExploreStats` when no bug is found, raises
    :class:`~detangle.BugFound` (an ``AssertionError``) otherwise.  Keyword
    options not listed here are :class:`~detangle.SimConfig` fields, e.g.
    ``timer_jitter=0.01`` or ``net={"drop": 0.1}``.
    """
    __tracebackhide__ = True
    cfg = build_config(config, options)
    name = name or _name_of(fn)
    runs = int(_settings.get("runs", runs))
    seed = _settings.get("seed", seed)
    max_duration = _settings.get("max_duration", max_duration)
    do_shrink = bool(_settings.get("shrink", shrink))
    replay = _settings.get("replay", replay)
    report_dir = _settings.get("report_dir", report_dir)
    db_setting = _settings.get("database", database)
    db = ExampleDatabase(db_setting) if isinstance(db_setting, str) and db_setting else None
    if db_setting is True:
        db = ExampleDatabase(_settings.DEFAULTS["database"])

    runner = _Runner(_resolve(fn), cfg)
    stats = ExploreStats(name=name)
    started = time.monotonic()

    if replay:
        values = decode_token(replay)
        result = runner.replay(values, trace=True)
        stats.record(result)
        stats.strategy = "replay"
        if result.failure is not None:
            report = BugReport(
                name=name,
                failure=result.failure,
                token=encode_token(values),
                values=values,
                runs=1,
                strategy="replay",
                seed=None,
                trace=result.trace,
                deviations=_count_non_seed(result),
                secondary=result.secondary,
                source="replay",
            )
            if report_dir:
                report.html_report = _write_html(report, report_dir)
            _raise(report)
        stats.elapsed = time.monotonic() - started
        return stats

    if db is not None:
        for token in db.fetch(name):
            try:
                values = decode_token(token)
            except ValueError:
                db.delete(name, token)
                continue
            stats.replayed_examples += 1
            result = runner.replay(values)
            if result.failure is not None:
                report = _finalize(
                    runner,
                    name,
                    result,
                    runs=1,
                    strategy_desc="saved example",
                    seed=None,
                    do_shrink=do_shrink,
                    source="database",
                    report_dir=report_dir,
                )
                if report.token != token:
                    db.delete(name, token)
                    db.save(name, report.token)
                _raise(report)
            db.delete(name, token)

    strat = make_strategy(strategy, seed)
    stats.strategy = strat.describe()
    stats.seed = getattr(strat, "seed", None)
    if isinstance(strat, Portfolio):
        stats.seed = getattr(strat.strategies[0], "seed", None)
    deadline = started + max_duration if max_duration else None
    for index in range(runs):
        result = runner.run(strat, index)
        stats.record(result)
        if result.failure is not None:
            desc = strat.current.describe() if isinstance(strat, Portfolio) else strat.describe()
            report = _finalize(
                runner,
                name,
                result,
                runs=index + 1,
                strategy_desc=desc,
                seed=stats.seed,
                do_shrink=do_shrink,
                source="exploration",
                report_dir=report_dir,
            )
            if db is not None:
                db.save(name, report.token)
            _raise(report)
        if strat.exhausted:
            stats.exhausted = not isinstance(strat, (FIFO, Replay))
            break
        if deadline is not None and time.monotonic() > deadline:
            break
    stats.elapsed = time.monotonic() - started
    if _settings.get("verbose"):
        print(stats.summary())
    return stats


@overload
def test(fn: Callable[..., Coroutine[Any, Any, Any]]) -> Callable[..., None]: ...


@overload
def test(
    fn: None = None,
    *,
    runs: int | None = ...,
    strategy: str | Strategy | None = ...,
    seed: int | None = ...,
    max_duration: float | None = ...,
    shrink: bool | None = ...,
    config: SimConfig | None = ...,
    database: str | bool | None = ...,
    report_dir: str | None = ...,
    **options: Any,
) -> Callable[[Callable[..., Coroutine[Any, Any, Any]]], Callable[..., None]]: ...


def test(fn: Any = None, **kwargs: Any) -> Any:
    """Decorator turning an ``async def`` test into a detangle exploration.

    The decorated function becomes a regular (synchronous) function, so it
    works with pytest, unittest or plain scripts.  Arguments passed to it
    (e.g. pytest fixtures) are forwarded to every run.  Build mutable state
    *inside* the test so that every run starts fresh.
    """
    kwargs.setdefault("database", True)

    def decorate(func: Callable[..., Coroutine[Any, Any, Any]]) -> Callable[..., None]:
        if not inspect.iscoroutinefunction(func):
            raise TypeError(f"@detangle.test expects an async def function, got {func!r}")
        name = _name_of(func)

        @functools.wraps(func)
        def wrapper(*args: Any, **fkwargs: Any) -> None:
            __tracebackhide__ = True
            target = functools.partial(func, *args, **fkwargs) if (args or fkwargs) else func
            explore(target, name=name, **kwargs)

        wrapper._detangle_inner = func  # type: ignore[attr-defined]
        wrapper._detangle_options = dict(kwargs)  # type: ignore[attr-defined]
        return wrapper

    if fn is not None:
        return decorate(fn)
    return decorate


def run(
    main: Callable[[], Coroutine[Any, Any, _T]] | Coroutine[Any, Any, _T],
    *,
    seed: int | None = None,
    strategy: str | Strategy | None = None,
    replay: str | None = None,
    config: SimConfig | None = None,
    trace: bool = False,
    **options: Any,
) -> _T:
    """Like :func:`asyncio.run`, but deterministic and in virtual time.

    With no *seed*/*strategy*, asyncio's own scheduling order is used (only
    time is virtual).  ``seed=`` picks one random schedule reproducibly;
    ``replay=`` re-runs a token printed by a failing exploration.
    """
    __tracebackhide__ = True
    cfg = build_config(config, options)
    strat: Strategy
    if replay is not None:
        strat = Replay(decode_token(replay))
    elif strategy is not None:
        strat = make_strategy(strategy, seed)
    elif seed is not None:
        strat = RandomWalk(seed)
    else:
        strat = FIFO()
    strat.start(0)
    result = execute(_resolve(main), config=cfg, strategy=strat, trace=trace)
    strat.finish(result.decisions, result.failure is not None)
    if result.failure is not None:
        if trace and result.trace is not None:
            print(result.trace.render(limit=0))
        exc = result.failure.exception
        if exc is not None:
            raise exc
        raise DetangleError(result.failure.message)
    return result.value  # type: ignore[no-any-return]


def replay(
    fn: Callable[..., Coroutine[Any, Any, Any]],
    token: str,
    *,
    config: SimConfig | None = None,
    raise_on_failure: bool = True,
    **options: Any,
) -> RunResult:
    """Re-run exactly the schedule described by *token*, with a full trace."""
    __tracebackhide__ = True
    inner = _resolve(fn)
    if config is None and not options:
        options = {
            k: v
            for k, v in getattr(fn, "_detangle_options", {}).items()
            if k in SimConfig.__dataclass_fields__ or k == "config"
        }
        config = options.pop("config", None)
    cfg = build_config(config, options)
    values = decode_token(token)
    strat = Replay(values)
    strat.start(0)
    result = execute(inner, config=cfg, strategy=strat, trace=True)
    if result.failure is not None and raise_on_failure:
        _raise(
            BugReport(
                name=_name_of(fn),
                failure=result.failure,
                token=token,
                values=values,
                runs=1,
                strategy="replay",
                seed=None,
                trace=result.trace,
                deviations=_count_non_seed(result),
                secondary=result.secondary,
                source="replay",
            )
        )
    return result
