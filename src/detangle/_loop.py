"""The deterministic, virtual-time asyncio event loop at the heart of detangle.

Design
======

asyncio runs *callbacks* ("handles").  Every step of every task is a handle
(``Task.__step`` / ``Task.__wakeup``), and so are future callbacks, timers
and ``loop.call_soon`` calls.  A stock loop runs ready handles in strict FIFO
order and sleeps until the next timer.

:class:`SimLoop` groups ready handles into *lanes*:

* one lane per task (all the steps of a task, in order);
* one shared *callback lane* for everything else (future callbacks, timers,
  protocol callbacks...), kept in FIFO order;
* one lane per simulated executor job (``run_in_executor``/``to_thread``).

Whenever more than one lane is runnable, the loop asks its
:class:`~detangle.strategies.Strategy` which one to run.  Answer ``0`` (the
lane whose head is oldest) reproduces asyncio's own order exactly; any other
answer models "that await took a bit longer than usual" -- the kind of timing
difference that exposes race conditions in production.

Time is virtual: when nothing is runnable the clock jumps straight to the
next timer, so ``await asyncio.sleep(3600)`` completes instantly.

Every decision is recorded, which makes any run exactly reproducible.
"""

from __future__ import annotations

import asyncio
import collections
import heapq
import logging
import sys
import threading
import weakref
from collections.abc import Callable, Sequence
from contextvars import Context
from typing import Any, TypeGuard, TypeVar

from . import _instrument
from ._choices import FAULT, SCHED, TIMER, Decision
from ._config import SimConfig
from ._context import current_host
from ._frames import (
    Location,
    best_user_frame,
    caller_location,
    coro_name,
    frame_location,
    is_default_task_name,
    task_frames,
)
from .errors import (
    DeadlockError,
    InvariantViolation,
    RealIOError,
    StepLimitExceeded,
    TimeLimitExceeded,
)
from .strategies import FIFO, Strategy

__all__ = ["Lane", "SimLoop", "TaskInfo", "current_loop"]

_T = TypeVar("_T")
logger = logging.getLogger("detangle")

_TASK_TYPES: tuple[type, ...] = (asyncio.Task,)
_py_task = getattr(asyncio.tasks, "_PyTask", None)
if _py_task is not None and _py_task is not asyncio.Task:
    _TASK_TYPES = (asyncio.Task, _py_task)

_HAS_TASK_CONTEXT = sys.version_info >= (3, 11)


class Lane:
    """A FIFO queue of ready handles that belong together."""

    __slots__ = ("id", "kind", "label", "queue", "task")

    def __init__(self, lane_id: int, task: asyncio.Task[Any] | None, kind: str, label: str) -> None:
        self.id = lane_id
        self.task = task
        self.kind = kind  # "task" | "callbacks" | "thread"
        self.label = label
        self.queue: collections.deque[tuple[int, asyncio.Handle]] = collections.deque()

    def __repr__(self) -> str:
        return f"<Lane {self.id} {self.label} ready={len(self.queue)}>"


def _is_task(obj: object) -> TypeGuard[asyncio.Task[Any]]:
    return isinstance(obj, _TASK_TYPES)


def _head_seq(lane: Lane) -> int:
    return lane.queue[0][0]


class TaskInfo:
    """Bookkeeping about a task created inside the simulation."""

    __slots__ = ("host", "id", "lane", "parent", "spawned_at", "task")

    def __init__(
        self,
        task: asyncio.Task[Any],
        lane: Lane,
        parent: asyncio.Task[Any] | None,
        host: str | None,
        spawned_at: Location | None,
    ) -> None:
        self.task = task
        self.id = lane.id
        self.lane = lane
        self.parent = parent
        self.host = host
        self.spawned_at = spawned_at


class _DeadHandle(asyncio.Handle):
    """Returned by an abandoned loop; never runs."""

    __slots__ = ()


def check_no_running_loop() -> None:
    if asyncio.events._get_running_loop() is not None:
        raise RuntimeError(
            "Cannot run a detangle simulation while another event loop is running "
            "in this thread. Call detangle from synchronous code (a plain `def` "
            "test), not from inside an `async def` running on another loop."
        )


def current_loop() -> SimLoop:
    """The running :class:`SimLoop`, or raise a helpful error."""
    loop = asyncio.events._get_running_loop()
    if not isinstance(loop, SimLoop):
        raise RuntimeError(
            "this detangle API must be called from code running inside a detangle "
            "simulation (detangle.run / detangle.explore / @detangle.test)"
        )
    return loop


class SimLoop(asyncio.AbstractEventLoop):
    """A deterministic asyncio event loop with virtual time.

    You rarely create one directly: :func:`detangle.run`,
    :func:`detangle.explore` and :func:`detangle.test` do it for you.
    """

    _detangle_sim = True
    slow_callback_duration = 0.1

    def __init__(
        self,
        config: SimConfig | None = None,
        strategy: Strategy | None = None,
        *,
        tracer: Any = None,
    ) -> None:
        _instrument.install()
        self.config = config or SimConfig()
        self._strategy: Strategy = strategy or FIFO()
        self.decisions: list[Decision] = []
        self.tracer = tracer
        self._recording = True
        self._limits = True
        self._now = 0.0
        self._seq = 0
        self._lane_counter = 0
        self._callback_lane = Lane(0, None, "callbacks", "callbacks")
        self._task_lanes: dict[asyncio.Task[Any], Lane] = {}
        self._active: dict[int, Lane] = {}
        self._timers: list[tuple[float, int, asyncio.TimerHandle]] = []
        self._cancelled_timers = 0
        self._inbox: collections.deque[tuple[asyncio.Handle, Any]] = collections.deque()
        self._inbox_lock = threading.Lock()
        self._running = False
        self._stopping = False
        self._closed = False
        self._abandoned = False
        self._thread_id: int | None = None
        self._old_agen_hooks: Any = None
        self._exception_handler: (
            Callable[[asyncio.AbstractEventLoop, dict[str, Any]], object] | None
        ) = None
        self._task_factory: Any = None
        self._debug = False
        self._default_executor: Any = None
        self._asyncgens: dict[int, weakref.ref[Any]] = {}
        self._agen_counter = 0
        self._asyncgens_shutdown_called = False
        self.tasks: list[asyncio.Task[Any]] = []
        self.task_info: dict[asyncio.Task[Any], TaskInfo] = {}
        self.steps = 0
        self.order_hash = 0
        self.unhandled: list[dict[str, Any]] = []
        self._invariants: list[tuple[str, Callable[[], object]]] = []
        self._interruptible: dict[asyncio.Task[Any], int] = {}
        self.injected: dict[asyncio.Task[Any], str] = {}
        self._holders: weakref.WeakKeyDictionary[Any, list[asyncio.Task[Any]]] = (
            weakref.WeakKeyDictionary()
        )
        self._current_lane: Lane | None = None
        self._net: Any = None
        self._time_patch: Any = None
        self.external_events = 0
        if self.config.patch_time:
            from ._timepatch import TimePatch

            self._time_patch = TimePatch(self)

    # ------------------------------------------------------------------
    # Decisions
    # ------------------------------------------------------------------

    @property
    def strategy(self) -> Strategy:
        return self._strategy

    @property
    def recording(self) -> bool:
        """False during teardown: every decision then takes its default."""
        return self._recording

    def _schedule(self, lanes: Sequence[Lane]) -> int:
        n = len(lanes)
        idx = self._strategy.schedule(lanes)
        if not 0 <= idx < n:
            idx = min(max(idx, 0), n - 1)
        self.decisions.append(Decision(SCHED, n, idx))
        if idx and self.tracer is not None:
            self.tracer.on_deviation(lanes[:idx])
        return idx

    def choose(self, n: int, kind: str) -> int:
        """Pick an integer in ``range(n)`` through the strategy (0 = default)."""
        if n <= 1 or not self._recording:
            return 0
        value = self._strategy.choose(n, kind)
        if not 0 <= value < n:
            value = min(max(value, 0), n - 1)
        self.decisions.append(Decision(kind, n, value))
        return value

    def flip(self, p: float, kind: str) -> bool:
        """Return True with probability ``p`` through the strategy (False = default)."""
        if p <= 0.0 or not self._recording:
            return False
        if p >= 1.0:
            return True
        value = bool(self._strategy.flip(p, kind))
        self.decisions.append(Decision(kind, 2, int(value)))
        return value

    # ------------------------------------------------------------------
    # Lanes and tasks
    # ------------------------------------------------------------------

    def _new_lane_id(self) -> int:
        self._lane_counter += 1
        return self._lane_counter

    def _lane_for_task(self, task: asyncio.Task[Any]) -> Lane:
        lane = self._task_lanes.get(task)
        if lane is None:
            lane = Lane(self._new_lane_id(), task, "task", "")
            self._task_lanes[task] = lane
            self.tasks.append(task)
            parent = asyncio.tasks.current_task(self) if self._running else None
            info = TaskInfo(task, lane, parent, current_host.get(), caller_location(1))
            self.task_info[task] = info
            if self._net is not None:
                self._net._on_task_created(task, info.host)
            if self.tracer is not None:
                self.tracer.on_spawn(task, info)
        return lane

    def label(self, task: asyncio.Task[Any] | None) -> str:
        """A short, stable, human readable name for a task."""
        if task is None:
            return "-"
        info = self.task_info.get(task)
        tid = info.id if info is not None else 0
        name = task.get_name()
        if not is_default_task_name(name):
            return name
        return f"{coro_name(task.get_coro())}#{tid}"

    def lane_label(self, lane: Lane) -> str:
        if lane.task is not None:
            return self.label(lane.task)
        return lane.label

    def _enqueue(self, handle: asyncio.Handle, callback: Any) -> None:
        owner = getattr(callback, "__self__", None)
        if owner is not None and _is_task(owner):
            lane = self._task_lanes.get(owner) or self._lane_for_task(owner)
        else:
            lane = self._callback_lane
        self._enqueue_lane(lane, handle)

    def _enqueue_lane(self, lane: Lane, handle: asyncio.Handle) -> None:
        self._seq += 1
        queue = lane.queue
        if not queue:
            self._active[lane.id] = lane
        queue.append((self._seq, handle))

    def _pick_lane(self) -> Lane | None:
        active = self._active
        if not active:
            return None
        lanes: list[Lane] = []
        for lane in list(active.values()):
            queue = lane.queue
            while queue and queue[0][1]._cancelled:
                queue.popleft()
            if queue:
                lanes.append(lane)
            else:
                del active[lane.id]
        if not lanes:
            return None
        if len(lanes) == 1:
            return lanes[0]
        lanes.sort(key=_head_seq)
        if not self.config.reorder or not self._recording:
            return lanes[0]
        return lanes[self._schedule(lanes)]

    # ------------------------------------------------------------------
    # The scheduler proper
    # ------------------------------------------------------------------

    def _run_once(self) -> bool:
        """Run a single handle.  Returns False when there is nothing left to do."""
        if self._inbox:
            self._drain_inbox()
        if self._timers and self._timers[0][0] <= self._now:
            self._collect_due_timers()
        lane = self._pick_lane()
        if lane is None:
            return self._advance_time()
        _, handle = lane.queue.popleft()
        if not lane.queue:
            del self._active[lane.id]
        self._execute(lane, handle)
        return True

    def _execute(self, lane: Lane, handle: asyncio.Handle) -> None:
        tracer = self.tracer
        if tracer is not None:
            tracer.before_step(lane, handle)
        if self.config.step_cost:
            self._now += self.config.step_cost
        self._current_lane = lane
        patch = self._time_patch
        if patch is not None:
            patch.enter()
        try:
            handle._run()
        finally:
            if patch is not None:
                patch.exit()
            self._current_lane = None
        self.steps += 1
        if lane.id:
            self.order_hash = hash((self.order_hash, lane.id))
        if tracer is not None:
            tracer.after_step(lane, handle)
        task = lane.task
        if task is not None and self._interruptible and task in self._interruptible:
            self._maybe_interrupt(task)
        if not self._limits:
            return
        if self._invariants:
            self._check_invariants()
        if self.steps >= self.config.max_steps:
            raise StepLimitExceeded(
                f"the run exceeded max_steps={self.config.max_steps} "
                f"(virtual time {self._now:.6f}s). This usually means a livelock, an "
                "infinite retry loop or a busy-wait on `await asyncio.sleep(0)`."
            )
        max_time = self.config.max_time
        if max_time is not None and self._now > max_time:
            raise TimeLimitExceeded(
                f"virtual time exceeded max_time={max_time}s after {self.steps} steps"
            )

    def _advance_time(self) -> bool:
        timers = self._timers
        while timers:
            when, _, handle = timers[0]
            if handle._cancelled:
                heapq.heappop(timers)
                handle._scheduled = False  # type: ignore[attr-defined]
                self._cancelled_timers -= 1
                continue
            self._now = max(self._now, when)
            self._collect_due_timers()
            return True
        return False

    def _collect_due_timers(self) -> None:
        timers = self._timers
        now = self._now
        while timers and timers[0][0] <= now:
            _, _, handle = heapq.heappop(timers)
            handle._scheduled = False  # type: ignore[attr-defined]
            if handle._cancelled:
                self._cancelled_timers -= 1
                continue
            self._enqueue(handle, handle._callback)  # type: ignore[attr-defined]

    def _drain_inbox(self) -> None:
        with self._inbox_lock:
            items = list(self._inbox)
            self._inbox.clear()
        for handle, callback in items:
            if not handle._cancelled:
                self._enqueue(handle, callback)

    def has_pending_work(self) -> bool:
        return bool(self._active) or bool(self._timers) or bool(self._inbox)

    # ------------------------------------------------------------------
    # Faults and invariants
    # ------------------------------------------------------------------

    def mark_interruptible(self, task: asyncio.Task[Any], points: int) -> None:
        """Let the strategy cancel *task* at one of its next ``points`` suspensions."""
        k = self.choose(points + 1, FAULT)
        if k:
            self._interruptible[task] = k

    def _maybe_interrupt(self, task: asyncio.Task[Any]) -> None:
        if task.done():
            self._interruptible.pop(task, None)
            return
        remaining = self._interruptible[task] - 1
        if remaining > 0:
            self._interruptible[task] = remaining
            return
        del self._interruptible[task]
        frame = best_user_frame(task_frames(task))
        where = str(frame_location(frame)) if frame is not None else "?"
        self.injected[task] = where
        task.cancel(msg="detangle: injected cancellation")
        if self.tracer is not None:
            self.tracer.on_fault(f"injected cancellation into {self.label(task)} at {where}")

    def add_invariant(self, check: Callable[[], object], name: str | None = None) -> None:
        label = name or str(getattr(check, "__qualname__", None) or repr(check))
        self._invariants.append((label, check))

    def _check_invariants(self) -> None:
        for name, check in list(self._invariants):
            try:
                ok = check()
            except AssertionError as exc:
                raise InvariantViolation(name, str(exc)) from exc
            except Exception as exc:
                raise InvariantViolation(name, f"{type(exc).__name__}: {exc}") from exc
            if ok is False:
                raise InvariantViolation(name)

    # ------------------------------------------------------------------
    # Lock ownership (fed by detangle._instrument)
    # ------------------------------------------------------------------

    def _note_acquire(self, primitive: Any) -> None:
        task = asyncio.tasks.current_task(self)
        if task is None:
            return
        holders = self._holders.get(primitive)
        if holders is None:
            holders = self._holders[primitive] = []
        holders.append(task)

    def _note_release(self, primitive: Any) -> None:
        holders = self._holders.get(primitive)
        if not holders:
            return
        task = asyncio.tasks.current_task(self)
        if task in holders:
            holders.remove(task)
        else:
            holders.pop(0)

    def holders_of(self, primitive: Any) -> list[asyncio.Task[Any]]:
        return list(self._holders.get(primitive, ()))

    # ------------------------------------------------------------------
    # Running and stopping
    # ------------------------------------------------------------------

    def _check_closed(self) -> None:
        if self._closed:
            raise RuntimeError("Event loop is closed")

    def _check_running(self) -> None:
        if self._running:
            raise RuntimeError("This event loop is already running")
        check_no_running_loop()

    def _enter_running(self) -> None:
        self._thread_id = threading.get_ident()
        self._old_agen_hooks = sys.get_asyncgen_hooks()
        sys.set_asyncgen_hooks(
            firstiter=self._asyncgen_firstiter_hook, finalizer=self._asyncgen_finalizer_hook
        )
        asyncio.events._set_running_loop(self)
        self._running = True

    def _exit_running(self) -> None:
        self._running = False
        self._thread_id = None
        asyncio.events._set_running_loop(None)
        if self._old_agen_hooks is not None:
            sys.set_asyncgen_hooks(*self._old_agen_hooks)
            self._old_agen_hooks = None

    def run_forever(self) -> None:
        self._check_closed()
        self._check_running()
        self._enter_running()
        try:
            while not self._stopping:
                if not self._run_once():
                    raise DeadlockError(
                        "run_forever(): nothing is runnable and no timer is pending; "
                        "a real event loop would hang forever"
                    )
        finally:
            self._stopping = False
            self._exit_running()

    def run_until_complete(self, future: Any) -> Any:
        self._check_closed()
        self._check_running()
        new_task = not asyncio.isfuture(future)
        fut: asyncio.Future[Any] = asyncio.ensure_future(future, loop=self)
        self._enter_running()
        try:
            while not fut.done():
                if self._stopping:
                    break
                if not self._run_once():
                    from ._deadlock import analyze

                    report = analyze(self, fut)
                    raise DeadlockError(report.render(), report)
        except BaseException:
            if new_task and fut.done() and not fut.cancelled():
                fut.exception()
            raise
        finally:
            self._stopping = False
            self._exit_running()
        if not fut.done():
            raise RuntimeError("Event loop stopped before Future completed.")
        return fut.result()

    def run_steps(self, budget: int, until: Callable[[], bool]) -> bool:
        """Run up to *budget* handles until *until()* is true.  Returns until()."""
        self._check_closed()
        self._check_running()
        self._enter_running()
        try:
            for _ in range(budget):
                if until():
                    return True
                if not self._run_once():
                    break
            return until()
        finally:
            self._exit_running()

    def run_ready(self, budget: int) -> None:
        """Run already-ready handles (never advancing time), at most *budget* of them."""
        self._check_closed()
        self._check_running()
        self._enter_running()
        try:
            for _ in range(budget):
                if self._inbox:
                    self._drain_inbox()
                if self._timers and self._timers[0][0] <= self._now:
                    self._collect_due_timers()
                lane = self._pick_lane()
                if lane is None:
                    return
                _, handle = lane.queue.popleft()
                if not lane.queue:
                    del self._active[lane.id]
                self._execute(lane, handle)
        finally:
            self._exit_running()

    def stop(self) -> None:
        self._stopping = True

    def is_running(self) -> bool:
        return self._running

    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._running:
            raise RuntimeError("Cannot close a running event loop")
        if self._closed:
            return
        self._closed = True
        self._active.clear()
        self._callback_lane.queue.clear()
        for lane in self._task_lanes.values():
            lane.queue.clear()
        self._timers.clear()
        self._inbox.clear()

    def abandon(self) -> None:
        """Silence the loop: tasks left pending will never run nor complain."""
        self._abandoned = True
        for task in self.tasks:
            if not task.done():
                try:
                    task._log_destroy_pending = False  # type: ignore[attr-defined]
                except AttributeError:  # pragma: no cover - exotic task types
                    pass

    async def shutdown_asyncgens(self) -> None:
        self._asyncgens_shutdown_called = True
        gens = [ref() for ref in list(self._asyncgens.values())]
        self._asyncgens.clear()
        live = [g for g in gens if g is not None]
        if not live:
            return
        results = await asyncio.gather(*[g.aclose() for g in live], return_exceptions=True)
        for result, agen in zip(results, live, strict=True):
            if isinstance(result, Exception):
                self.call_exception_handler(
                    {
                        "message": f"an error occurred while closing async generator {agen!r}",
                        "exception": result,
                        "asyncgen": agen,
                    }
                )

    async def shutdown_default_executor(self, timeout: float | None = None) -> None:
        return None

    def _asyncgen_firstiter_hook(self, agen: Any) -> None:
        self._agen_counter += 1
        key = self._agen_counter
        gens = self._asyncgens

        def forget(_ref: object, key: int = key) -> None:
            gens.pop(key, None)

        gens[key] = weakref.ref(agen, forget)

    def _asyncgen_finalizer_hook(self, agen: Any) -> None:
        if self._closed or self._abandoned:
            return
        self.call_soon_threadsafe(self.create_task, agen.aclose())

    # ------------------------------------------------------------------
    # Scheduling callbacks
    # ------------------------------------------------------------------

    def _dead_handle(self, callback: Any, args: Sequence[Any]) -> asyncio.Handle:
        handle = _DeadHandle(callback, tuple(args), self, None)
        handle._cancelled = True
        return handle

    def call_soon(  # type: ignore[override]
        self, callback: Callable[..., object], *args: Any, context: Context | None = None
    ) -> asyncio.Handle:
        if self._abandoned:
            return self._dead_handle(callback, args)
        self._check_closed()
        handle = asyncio.Handle(callback, args, self, context)
        self._enqueue(handle, callback)
        return handle

    def call_soon_threadsafe(  # type: ignore[override]
        self, callback: Callable[..., object], *args: Any, context: Context | None = None
    ) -> asyncio.Handle:
        if self._thread_id is None or threading.get_ident() == self._thread_id:
            return self.call_soon(callback, *args, context=context)
        if self._abandoned or self._closed:
            return self._dead_handle(callback, args)
        handle = asyncio.Handle(callback, args, self, context)
        with self._inbox_lock:
            self._inbox.append((handle, callback))
        self.external_events += 1
        return handle

    def call_later(  # type: ignore[override]
        self,
        delay: float,
        callback: Callable[..., object],
        *args: Any,
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        if delay is None:
            raise TypeError("delay must not be None")
        return self.call_at(self._now + delay, callback, *args, context=context)

    def call_at(  # type: ignore[override]
        self,
        when: float,
        callback: Callable[..., object],
        *args: Any,
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        if when is None:
            raise TypeError("when cannot be None")
        if self._abandoned:
            timer = asyncio.TimerHandle(when, callback, args, self, context)
            timer._cancelled = True
            return timer
        self._check_closed()
        timer = asyncio.TimerHandle(when, callback, args, self, context)
        fire_at = when
        jitter = self.config.timer_jitter
        if jitter and self._recording:
            steps = self.config.jitter_steps
            k = self.choose(steps, TIMER)
            if k:
                fire_at = when + jitter * k / (steps - 1)
        self._push_timer(fire_at, timer)
        return timer

    def call_at_exact(
        self,
        when: float,
        callback: Callable[..., object],
        *args: Any,
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        """Like :meth:`call_at` but never jittered (used by the network)."""
        timer = asyncio.TimerHandle(when, callback, args, self, context)
        if self._abandoned:
            timer._cancelled = True
            return timer
        self._check_closed()
        self._push_timer(when, timer)
        return timer

    def _push_timer(self, fire_at: float, timer: asyncio.TimerHandle) -> None:
        self._seq += 1
        heapq.heappush(self._timers, (fire_at, self._seq, timer))
        timer._scheduled = True  # type: ignore[attr-defined]

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        if getattr(handle, "_scheduled", False):
            self._cancelled_timers += 1
            if self._cancelled_timers > 256 and self._cancelled_timers * 2 > len(self._timers):
                live = []
                for entry in self._timers:
                    if entry[2]._cancelled:
                        entry[2]._scheduled = False  # type: ignore[attr-defined]
                    else:
                        live.append(entry)
                heapq.heapify(live)
                self._timers = live
                self._cancelled_timers = 0

    def time(self) -> float:
        return self._now

    # ------------------------------------------------------------------
    # Futures and tasks
    # ------------------------------------------------------------------

    def create_future(self) -> asyncio.Future[Any]:
        return asyncio.Future(loop=self)

    def create_task(
        self,
        coro: Any,
        *,
        name: str | None = None,
        context: Context | None = None,
        **kwargs: Any,
    ) -> asyncio.Task[Any]:
        self._check_closed()
        token = None
        if context is not None and current_host.get() != context.get(current_host):
            token = current_host.set(context.get(current_host))
        try:
            if self._task_factory is None:
                if context is not None or kwargs:
                    task = asyncio.Task(coro, loop=self, name=name, context=context, **kwargs)  # type: ignore[call-arg]
                else:
                    task = asyncio.Task(coro, loop=self, name=name)
            else:
                if context is None:
                    task = self._task_factory(self, coro, **kwargs)
                else:
                    task = self._task_factory(self, coro, context=context, **kwargs)
                if name is not None:
                    task.set_name(name)
            if task not in self.task_info:
                self._lane_for_task(task)
        finally:
            if token is not None:
                current_host.reset(token)
        return task

    def set_task_factory(self, factory: Any) -> None:
        if factory is not None and not callable(factory):
            raise TypeError("task factory must be a callable or None")
        self._task_factory = factory

    def get_task_factory(self) -> Any:
        return self._task_factory

    # ------------------------------------------------------------------
    # Executors (simulated: jobs run on the loop thread, in their own lane)
    # ------------------------------------------------------------------

    def run_in_executor(  # type: ignore[override]
        self, executor: Any, func: Callable[..., _T], *args: Any
    ) -> asyncio.Future[_T]:
        self._check_closed()
        fut: asyncio.Future[_T] = self.create_future()
        name = getattr(func, "__qualname__", None) or getattr(func, "__name__", None)
        if name is None:
            inner = getattr(func, "args", None)  # functools.partial(ctx.run, f, ...)
            if inner:
                target = inner[0]
                name = getattr(target, "__qualname__", None) or type(target).__name__
            else:
                name = type(func).__name__
        lane = Lane(self._new_lane_id(), None, "thread", f"thread:{name}")
        lane.label = f"thread:{name}#{lane.id}"

        def job() -> None:
            if fut.cancelled():
                return
            try:
                result = func(*args)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                if not fut.cancelled():
                    fut.set_exception(exc)
            else:
                if not fut.cancelled():
                    fut.set_result(result)

        self._enqueue_lane(lane, asyncio.Handle(job, (), self, None))
        return fut

    def set_default_executor(self, executor: Any) -> None:
        self._default_executor = executor

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    def get_exception_handler(self) -> Any:
        return self._exception_handler

    def set_exception_handler(self, handler: Any) -> None:
        if handler is not None and not callable(handler):
            raise TypeError(f"A callable object or None is expected, got {handler!r}")
        self._exception_handler = handler

    def default_exception_handler(self, context: dict[str, Any]) -> None:
        message = context.get("message") or "Unhandled exception in event loop"
        exception = context.get("exception")
        exc_info: Any = (
            (type(exception), exception, exception.__traceback__) if exception else False
        )
        logger.error("%s", message, exc_info=exc_info)

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        if self._exception_handler is not None:
            try:
                self._exception_handler(self, context)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self.unhandled.append(
                    {"message": "Unhandled error in exception handler", "exception": exc}
                )
            return
        if self._closed or self._abandoned:
            return
        self.unhandled.append(context)
        if self.tracer is not None:
            self.tracer.on_unhandled(context)

    def get_debug(self) -> bool:
        return self._debug

    def set_debug(self, enabled: bool) -> None:
        self._debug = enabled

    # ------------------------------------------------------------------
    # Network (simulated)
    # ------------------------------------------------------------------

    @property
    def net(self) -> Any:
        """The :class:`detangle.net.SimNetwork` of this simulation."""
        if self._net is None:
            from .net import SimNetwork

            self._net = SimNetwork(self, self.config.net)
            for task, info in self.task_info.items():
                self._net._on_task_created(task, info.host)
        return self._net

    async def getaddrinfo(
        self,
        host: Any,
        port: Any,
        *,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> Any:
        return await self.net.getaddrinfo(
            host, port, family=family, type=type, proto=proto, flags=flags
        )

    async def getnameinfo(self, sockaddr: Any, flags: int = 0) -> Any:
        return (str(sockaddr[0]), str(sockaddr[1]))

    async def create_connection(
        self, protocol_factory: Any, host: Any = None, port: Any = None, **kwargs: Any
    ) -> Any:
        return await self.net.create_connection(protocol_factory, host, port, **kwargs)

    async def create_server(
        self, protocol_factory: Any, host: Any = None, port: Any = None, **kwargs: Any
    ) -> Any:
        return await self.net.create_server(protocol_factory, host, port, **kwargs)

    async def create_unix_connection(
        self, protocol_factory: Any, path: Any = None, **kwargs: Any
    ) -> Any:
        return await self.net.create_connection(protocol_factory, f"unix:{path}", 0, **kwargs)

    async def create_unix_server(
        self, protocol_factory: Any, path: Any = None, **kwargs: Any
    ) -> Any:
        return await self.net.create_server(protocol_factory, f"unix:{path}", 0, **kwargs)

    async def create_datagram_endpoint(
        self, protocol_factory: Any, local_addr: Any = None, remote_addr: Any = None, **kwargs: Any
    ) -> Any:
        return await self.net.create_datagram_endpoint(
            protocol_factory, local_addr, remote_addr, **kwargs
        )

    # ------------------------------------------------------------------
    # Real I/O is not available inside a simulation
    # ------------------------------------------------------------------

    def _real_io(self, what: str) -> RealIOError:
        return RealIOError(
            f"loop.{what}() performs real I/O, which cannot be simulated deterministically. "
            "Use asyncio streams/protocols (served by detangle's simulated network) or "
            "mock this dependency in your test."
        )

    def add_reader(self, *args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        raise self._real_io("add_reader")

    def remove_reader(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("remove_reader")

    def add_writer(self, *args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        raise self._real_io("add_writer")

    def remove_writer(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("remove_writer")

    def add_signal_handler(self, *args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        raise self._real_io("add_signal_handler")

    def remove_signal_handler(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("remove_signal_handler")

    async def sock_recv(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("sock_recv")

    async def sock_recv_into(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("sock_recv_into")

    async def sock_recvfrom(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("sock_recvfrom")

    async def sock_recvfrom_into(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("sock_recvfrom_into")

    async def sock_sendall(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("sock_sendall")

    async def sock_sendto(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("sock_sendto")

    async def sock_connect(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("sock_connect")

    async def sock_accept(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("sock_accept")

    async def sock_sendfile(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("sock_sendfile")

    async def sendfile(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("sendfile")

    async def start_tls(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("start_tls")

    async def connect_accepted_socket(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("connect_accepted_socket")

    async def connect_read_pipe(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("connect_read_pipe")

    async def connect_write_pipe(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("connect_write_pipe")

    async def subprocess_exec(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("subprocess_exec")

    async def subprocess_shell(self, *args: Any, **kwargs: Any) -> Any:
        raise self._real_io("subprocess_shell")
