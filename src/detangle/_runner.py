"""Execute one simulated run and classify its outcome."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ._choices import SEED, Decision
from ._config import SimConfig
from ._deadlock import DeadlockReport
from ._frames import Location, coro_name, traceback_user_location
from ._loop import SimLoop, check_no_running_loop
from ._trace import Trace, Tracer
from .errors import DeadlockError, InvariantViolation, StepLimitExceeded, TimeLimitExceeded
from .strategies import Strategy

__all__ = ["Failure", "RunResult", "execute"]

TEARDOWN_BUDGET = 50_000


@dataclass
class Failure:
    """Why a run failed."""

    kind: str
    """``exception``, ``deadlock``, ``invariant``, ``step-limit``, ``time-limit``,
    ``unobserved-exception``, ``callback-error``, ``task-leak`` or
    ``nondeterminism``."""
    message: str
    exception: BaseException | None = None
    location: Location | None = None
    task: str | None = None
    deadlock: DeadlockReport | None = None

    def signature(self) -> tuple[Any, ...]:
        """Two failures with the same signature are "the same bug"."""
        if self.deadlock is not None:
            return self.deadlock.signature()
        exc_type = type(self.exception).__qualname__ if self.exception is not None else None
        loc = (self.location.filename, self.location.function) if self.location else None
        return (self.kind, exc_type, loc)

    def headline(self) -> str:
        """One line describing the failure."""
        if self.kind == "deadlock":
            return "Deadlock"
        if self.exception is not None:
            message = str(self.exception).strip()
            first = message.splitlines()[0] if message else ""
            if first != message:
                first += " ..."
            return f"{type(self.exception).__name__}: {first}".strip().rstrip(":")
        return self.message


@dataclass
class RunResult:
    """Outcome of a single simulated run."""

    decisions: list[Decision]
    failure: Failure | None
    value: Any = None
    steps: int = 0
    virtual_time: float = 0.0
    tasks: int = 0
    leaked: list[str] = field(default_factory=list)
    secondary: list[Failure] = field(default_factory=list)
    trace: Trace | None = None
    faults_injected: int = 0
    order_hash: int = 0

    @property
    def ok(self) -> bool:
        return self.failure is None

    @property
    def values(self) -> list[int]:
        return [d.value for d in self.decisions]

    def schedule_key(self) -> int:
        """Identifies the interleaving: the order in which task steps ran."""
        return self.order_hash


def _exception_failure(
    exc: BaseException, kind: str = "exception", task: str | None = None
) -> Failure:
    loc = traceback_user_location(exc.__traceback__)
    return Failure(
        kind=kind, message=f"{type(exc).__name__}: {exc}", exception=exc, location=loc, task=task
    )


def execute(
    fn: Callable[..., Any],
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
    *,
    config: SimConfig | None = None,
    strategy: Strategy | None = None,
    trace: bool = False,
    setup: Callable[[SimLoop], None] | None = None,
) -> RunResult:
    """Run ``fn(*args, **kwargs)`` (a coroutine function) once in a fresh simulation."""
    config = config or SimConfig()
    check_no_running_loop()
    tracer = Tracer() if trace else None
    loop = SimLoop(config, strategy, tracer=tracer)
    if tracer is not None:
        tracer.attach(loop)
    saved_random = random.getstate() if config.seed_random else None
    failure: Failure | None = None
    secondary: list[Failure] = []
    value: Any = None
    main: asyncio.Task[Any] | None = None
    try:
        if config.seed_random:
            random.seed(loop.choose(1 << 32, SEED))
        if setup is not None:
            setup(loop)
        coro = fn(*args, **(kwargs or {})) if callable(fn) else fn
        if not asyncio.iscoroutine(coro):
            raise TypeError(
                f"detangle expected a coroutine function, but {fn!r} returned {type(coro).__name__}"
            )
        main = loop.create_task(coro, name=coro_name(coro))
        try:
            value = loop.run_until_complete(main)
        except DeadlockError as exc:
            failure = Failure("deadlock", str(exc), exc, None, None, exc.report)
        except InvariantViolation as exc:
            failure = _exception_failure(exc, "invariant")
        except StepLimitExceeded as exc:
            failure = Failure("step-limit", str(exc), exc)
        except TimeLimitExceeded as exc:
            failure = Failure("time-limit", str(exc), exc)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:
            failure = _exception_failure(exc, task=loop.label(main))

        leaked = _teardown(loop, main)
        secondary.extend(_collect_background_failures(loop, main))
        if leaked and config.check_leaks:
            secondary.append(
                Failure(
                    "task-leak",
                    f"{len(leaked)} task(s) still pending when the main coroutine returned: "
                    + ", ".join(leaked[:8]),
                )
            )
        if failure is None and secondary:
            failure, secondary = secondary[0], secondary[1:]
        return RunResult(
            decisions=loop.decisions,
            failure=failure,
            value=value if failure is None else None,
            steps=loop.steps,
            virtual_time=loop.time(),
            tasks=len(loop.tasks),
            leaked=leaked,
            secondary=secondary,
            trace=tracer.build() if tracer is not None else None,
            faults_injected=len(loop.injected),
            order_hash=loop.order_hash,
        )
    finally:
        loop.abandon()
        if not loop.is_running():
            loop.close()
        if saved_random is not None:
            random.setstate(saved_random)


def _teardown(loop: SimLoop, main: asyncio.Task[Any] | None) -> list[str]:
    """asyncio.run()-style shutdown: cancel what is left, drain, close generators."""
    loop._recording = False
    loop._limits = False
    loop.tracer = None
    leftovers = [t for t in loop.tasks if not t.done() and t is not main]
    leaked = [loop.label(t) for t in leftovers]
    if main is not None and not main.done():
        leftovers.append(main)
    for task in leftovers:
        task.cancel()
    try:
        if leftovers:
            loop.run_steps(TEARDOWN_BUDGET, lambda: all(t.done() for t in leftovers))
        # Finalizers of abandoned async generators schedule aclose() tasks.
        loop.run_ready(TEARDOWN_BUDGET)
        if loop._asyncgens:
            agen_task = loop.create_task(loop.shutdown_asyncgens())
            loop.run_steps(TEARDOWN_BUDGET, agen_task.done)
        loop.run_ready(TEARDOWN_BUDGET)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as exc:  # pragma: no cover - teardown must never mask the real failure
        loop.unhandled.append({"message": "error during simulation teardown", "exception": exc})
    return leaked


def _collect_background_failures(loop: SimLoop, main: asyncio.Task[Any] | None) -> list[Failure]:
    failures: list[Failure] = []
    config = loop.config
    for task in loop.tasks:
        if task is main or not task.done() or task.cancelled():
            continue
        if not getattr(task, "_log_traceback", False):
            continue
        exc = task.exception()  # marks it as retrieved: asyncio will not log it again
        if exc is None or not config.fail_on_unobserved:
            continue
        label = loop.label(task)
        failure = _exception_failure(exc, "unobserved-exception", task=label)
        failure.message = (
            f"exception in background task {label} was never retrieved: {failure.message}"
        )
        failures.append(failure)
    if config.fail_on_callback_error:
        for context in loop.unhandled:
            exc = context.get("exception")
            message = str(context.get("message") or "unhandled error in callback")
            if exc is not None:
                failure = _exception_failure(exc, "callback-error")
                failure.message = f"{message}: {type(exc).__name__}: {exc}"
            else:
                failure = Failure("callback-error", message)
            failures.append(failure)
    return failures
