"""Linearizability checking, Jepsen/Porcupine style.

Record what concurrent clients *invoked* and what they *observed*; then ask
whether some sequential execution of a simple model explains every
observation while respecting real-time order ("if A returned before B was
invoked, A happened before B").  If none does, the system under test is not
linearizable -- a stale read, a lost write, a duplicated dequeue...

The checker implements the Wing & Gong search with Lowe's memoisation and
P-compositionality (Horn & Kroening), as popularised by Knossos and
Porcupine.  Operations whose outcome is unknown (timeouts, crashes) may take
effect at any point after their invocation -- or never.

Example::

    history = detangle.History()

    async def client(i):
        await history.call(i, "put", ("x", i), kv.put("x", i))
        await history.call(i, "get", "x", kv.get("x"))

    await asyncio.gather(*(client(i) for i in range(3)))
    history.assert_linearizable(detangle.lin.KV())
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Hashable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

from .errors import NotLinearizable

__all__ = [
    "KV",
    "Counter",
    "FIFOQueue",
    "History",
    "LinearizabilityResult",
    "Model",
    "Mutex",
    "Operation",
    "Register",
    "Set",
    "check",
]

_T = TypeVar("_T")

OK, FAIL, INFO, PENDING = "ok", "fail", "info", "pending"


@dataclass(eq=False)
class Operation:
    """One client operation: invocation, and (maybe) its completion."""

    index: int
    process: Hashable
    f: str
    arg: Any
    call: int
    call_time: float
    output: Any = None
    status: str = PENDING
    ret: int | None = None
    ret_time: float | None = None

    def describe(self) -> str:
        arg = "" if self.arg is None else _fmt_arg(self.arg)
        text = f"{self.f}({arg})"
        if self.status == OK:
            text += f" -> {self.output!r}"
        elif self.status == INFO:
            text += " -> ?"
        elif self.status == FAIL:
            text += " -> failed"
        return text

    def __repr__(self) -> str:
        span = f"[{self.call}..{self.ret}]"
        return f"<Operation #{self.index} {self.process!r} {self.describe()} {span}>"


def _fmt_arg(arg: Any) -> str:
    if isinstance(arg, tuple):
        return ", ".join(repr(a) for a in arg)
    return repr(arg)


def _now() -> float:
    loop = asyncio.events._get_running_loop()
    return loop.time() if loop is not None else 0.0


class History:
    """A concurrent history of operations, recorded as they happen."""

    def __init__(self) -> None:
        self.operations: list[Operation] = []
        self._clock = 0

    def _tick(self) -> int:
        self._clock += 1
        return self._clock

    def invoke(self, process: Hashable, f: str, arg: Any = None) -> Operation:
        """Record that *process* started operation *f(arg)*."""
        op = Operation(len(self.operations), process, f, arg, self._tick(), _now())
        self.operations.append(op)
        return op

    def ok(self, op: Operation, output: Any = None) -> None:
        """Record that *op* completed and observed *output*."""
        self._complete(op, OK, output)

    def fail(self, op: Operation) -> None:
        """Record that *op* definitely did **not** take effect."""
        self._complete(op, FAIL, None)

    def info(self, op: Operation) -> None:
        """Record that the outcome of *op* is unknown (it may or may not have happened)."""
        self._complete(op, INFO, None)

    def _complete(self, op: Operation, status: str, output: Any) -> None:
        if op.status != PENDING:
            raise ValueError(f"{op!r} was already completed")
        op.status = status
        op.output = output
        if status == OK:
            op.ret = self._tick()
            op.ret_time = _now()

    async def call(
        self,
        process: Hashable,
        f: str,
        arg: Any,
        awaitable: Awaitable[_T],
        *,
        fail_on: tuple[type[BaseException], ...] = (),
    ) -> _T:
        """Invoke, await *awaitable*, and record the outcome.

        Exceptions listed in *fail_on* mean "definitely did not happen";
        any other exception (timeouts, cancellation...) records an operation
        with an unknown outcome.  The exception is re-raised either way.
        """
        op = self.invoke(process, f, arg)
        try:
            result = await awaitable
        except fail_on:
            self.fail(op)
            raise
        except BaseException:
            self.info(op)
            raise
        self.ok(op, result)
        return result

    def check(self, model: Model) -> LinearizabilityResult:
        return check(self.operations, model)

    def assert_linearizable(self, model: Model) -> LinearizabilityResult:
        """Raise :class:`~detangle.NotLinearizable` if the history is not linearizable."""
        __tracebackhide__ = True
        result = self.check(model)
        if not result.ok:
            raise NotLinearizable(result.explain(), result)
        return result

    def __len__(self) -> int:
        return len(self.operations)


# ----------------------------------------------------------------------------
# Models
# ----------------------------------------------------------------------------


class Model:
    """A sequential specification.

    Subclasses implement :meth:`init` and :meth:`step`.  States must be
    hashable (use tuples, frozensets...).
    """

    name = "model"

    def init(self) -> Hashable:
        raise NotImplementedError

    def step(self, state: Any, op: Operation) -> tuple[bool, Any]:
        """Apply *op* to *state*.  Return ``(legal, new_state)``.

        For operations with an unknown outcome (``op.status == "info"``) the
        output is unknown: accept any output.
        """
        raise NotImplementedError

    def partition(self, operations: Sequence[Operation]) -> list[list[Operation]]:
        """Split into independent sub-histories (P-compositionality)."""
        return [list(operations)]

    def describe_state(self, state: Any) -> str:
        return repr(state)


def _known(op: Operation) -> bool:
    return op.status == OK


class Register(Model):
    """A single read/write register with optional compare-and-set.

    Operations: ``read``/``get`` -> value, ``write``/``put``/``set`` (value),
    ``cas`` ((expected, new)) -> bool.
    """

    name = "register"

    def __init__(self, initial: Any = None) -> None:
        self.initial = initial

    def init(self) -> Hashable:
        state: Hashable = _freeze(self.initial)
        return state

    def step(self, state: Any, op: Operation) -> tuple[bool, Any]:
        f = op.f
        if f in ("read", "get"):
            return (not _known(op) or _freeze(op.output) == state, state)
        if f in ("write", "put", "set"):
            return (True, _freeze(op.arg))
        if f == "cas":
            expected, new = op.arg
            success = state == _freeze(expected)
            if _known(op) and op.output is not None and bool(op.output) != success:
                return (False, state)
            if not _known(op) and not success:
                return (True, state)
            return (True, _freeze(new) if success else state)
        raise ValueError(f"Register: unknown operation {f!r}")


class KV(Model):
    """A key-value map, checked key by key (P-compositionality).

    Operations (``arg`` in parentheses): ``get`` (key) -> value or None,
    ``put``/``set`` ((key, value)), ``delete`` (key), ``cas``
    ((key, expected, new)) -> bool.
    """

    name = "kv"

    def __init__(self, initial: dict[Any, Any] | None = None) -> None:
        self.initial = dict(initial or {})

    def _key(self, op: Operation) -> Any:
        return op.arg[0] if isinstance(op.arg, tuple) else op.arg

    def partition(self, operations: Sequence[Operation]) -> list[list[Operation]]:
        groups: dict[Any, list[Operation]] = {}
        for op in operations:
            groups.setdefault(_freeze(self._key(op)), []).append(op)
        return list(groups.values())

    def init(self) -> Hashable:
        return ("__detangle_kv_init__",)

    def step(self, state: Any, op: Operation) -> tuple[bool, Any]:
        key = self._key(op)
        if state == ("__detangle_kv_init__",):
            state = _freeze(self.initial.get(key))
        f = op.f
        if f in ("get", "read"):
            return (not _known(op) or _freeze(op.output) == state, state)
        if f in ("put", "set", "write"):
            return (True, _freeze(op.arg[1]))
        if f == "delete":
            return (True, None)
        if f == "cas":
            _, expected, new = op.arg
            success = state == _freeze(expected)
            if _known(op) and op.output is not None and bool(op.output) != success:
                return (False, state)
            if not _known(op) and not success:
                return (True, state)
            return (True, _freeze(new) if success else state)
        raise ValueError(f"KV: unknown operation {f!r}")


class Counter(Model):
    """A counter: ``add``/``incr`` (n, default 1) -> new value or None, ``read`` -> value."""

    name = "counter"

    def __init__(self, initial: int = 0) -> None:
        self.initial = initial

    def init(self) -> Hashable:
        return self.initial

    def step(self, state: Any, op: Operation) -> tuple[bool, Any]:
        if op.f in ("add", "incr", "increment"):
            new = state + (1 if op.arg is None else op.arg)
            if _known(op) and op.output is not None and op.output != new:
                return (False, state)
            return (True, new)
        if op.f in ("read", "get"):
            return (not _known(op) or op.output == state, state)
        raise ValueError(f"Counter: unknown operation {op.f!r}")


class Set(Model):
    """A set: ``add`` (x) -> bool|None, ``remove`` (x) -> bool|None,
    ``contains`` (x) -> bool, ``read`` -> collection."""

    name = "set"

    def init(self) -> Hashable:
        return frozenset()

    def step(self, state: Any, op: Operation) -> tuple[bool, Any]:
        f, x = op.f, op.arg
        if f == "add":
            present = x in state
            if _known(op) and op.output is not None and bool(op.output) == present:
                return (False, state)
            return (True, state | {x})
        if f in ("remove", "discard"):
            present = x in state
            if _known(op) and op.output is not None and bool(op.output) != present:
                return (False, state)
            return (True, state - {x})
        if f == "contains":
            return (not _known(op) or bool(op.output) == (x in state), state)
        if f == "read":
            return (not _known(op) or frozenset(op.output) == state, state)
        raise ValueError(f"Set: unknown operation {f!r}")


class FIFOQueue(Model):
    """A FIFO queue: ``enqueue``/``put`` (x), ``dequeue``/``get`` -> x (None = empty)."""

    name = "queue"

    def init(self) -> Hashable:
        return ()

    def step(self, state: Any, op: Operation) -> tuple[bool, Any]:
        if op.f in ("enqueue", "put", "push"):
            return (True, (*state, _freeze(op.arg)))
        if op.f in ("dequeue", "get", "pop"):
            if not state:
                return (not _known(op) or op.output is None, state)
            if _known(op) and _freeze(op.output) != state[0]:
                return (False, state)
            return (True, state[1:])
        raise ValueError(f"FIFOQueue: unknown operation {op.f!r}")


class Mutex(Model):
    """A lock: ``acquire`` / ``release`` by process.  State = holder or None."""

    name = "mutex"

    def init(self) -> Hashable:
        return None

    def step(self, state: Any, op: Operation) -> tuple[bool, Any]:
        if op.f == "acquire":
            if _known(op) and op.output is False:
                return (True, state)
            return (state is None, op.process)
        if op.f == "release":
            return (state == op.process, None)
        raise ValueError(f"Mutex: unknown operation {op.f!r}")


def _freeze(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze(v)) for k, v in value.items()))
    if isinstance(value, set):
        return frozenset(value)
    return value


# ----------------------------------------------------------------------------
# The checker
# ----------------------------------------------------------------------------


class _Entry:
    __slots__ = ("is_call", "match", "next", "op", "prev", "unknown")

    def __init__(self, op: Operation | None, is_call: bool, unknown: bool = False) -> None:
        self.op = op
        self.is_call = is_call
        self.unknown = unknown
        self.match: _Entry | None = None
        self.prev: _Entry | None = None
        self.next: _Entry | None = None


@dataclass
class PartitionResult:
    ok: bool
    operations: list[Operation]
    linearization: list[Operation] = field(default_factory=list)
    stuck_on: Operation | None = None
    state: Any = None
    explored: int = 0


@dataclass
class LinearizabilityResult:
    """Outcome of :func:`check`."""

    ok: bool
    model: Model
    partitions: list[PartitionResult]

    @property
    def counterexample(self) -> PartitionResult | None:
        for part in self.partitions:
            if not part.ok:
                return part
        return None

    def explain(self) -> str:
        bad = self.counterexample
        if bad is None:
            return f"history is linearizable ({self.model.name})"
        lines = [f"history is not linearizable with respect to the {self.model.name} model"]
        n = len(bad.operations)
        lines.append(f"longest linearizable prefix ({len(bad.linearization)} of {n} operations):")
        for i, op in enumerate(bad.linearization, 1):
            lines.append(f"  {i:>3}. {op.process!r}: {op.describe()}")
        if not bad.linearization:
            lines.append("  (none)")
        lines.append(f"  state afterwards: {self.model.describe_state(bad.state)}")
        if bad.stuck_on is not None:
            stuck = bad.stuck_on
            lines.append(
                f"no valid ordering can explain: {stuck.process!r}: {stuck.describe()} "
                f"(invoked at event {stuck.call}, returned at event {stuck.ret})"
            )
        timeline = render_timeline(bad.operations)
        if timeline:
            lines.append("timeline (real-time order, left to right; > = never returned):")
            lines.append(timeline)
        return "\n".join(lines)


def render_timeline(operations: Sequence[Operation], max_ops: int = 60) -> str:
    """Compact ASCII timeline: one row per process, one numbered bar per operation."""
    ops = [op for op in operations if op.status in (OK, INFO)]
    if not ops or len(ops) > max_ops:
        return ""
    points = sorted({op.call for op in ops} | {op.ret for op in ops if op.ret is not None})
    col = {p: i * 3 for i, p in enumerate(points)}
    end = len(points) * 3 + 2
    processes: list[Hashable] = []
    for op in ops:
        if op.process not in processes:
            processes.append(op.process)
    name_w = max(len(repr(p)) for p in processes)
    rows = []
    number = {op.index: i for i, op in enumerate(ops, 1)}
    for proc in processes:
        row = [" "] * (end + 4)
        for op in ops:
            if op.process != proc:
                continue
            start = col[op.call]
            stop = col[op.ret] if op.ret is not None else end
            label = str(number[op.index])
            stop = max(stop, start + len(label) + 1)
            row[start] = "["
            for i in range(start + 1, stop):
                row[i] = "-"
            row[stop] = "]" if op.ret is not None else ">"
            mid = start + 1 + max(0, (stop - start - 1 - len(label)) // 2)
            for i, ch in enumerate(label):
                row[mid + i] = ch
        rows.append(f"  {proc!r:>{name_w}} " + "".join(row).rstrip())
    rows.append("")
    for op in ops:
        rows.append(f"  {number[op.index]:>3}  {op.process!r}: {op.describe()}")
    return "\n".join(rows)


def _check_partition(model: Model, operations: list[Operation], budget: int) -> PartitionResult:
    ops = [op for op in operations if op.status in (OK, INFO, PENDING)]
    if not ops:
        return PartitionResult(True, list(operations))
    bit_of = {op.index: 1 << i for i, op in enumerate(ops)}
    horizon = (
        max(op.ret for op in ops if op.ret is not None) + 1 if any(op.ret for op in ops) else 1
    )
    events: list[tuple[float, int, _Entry]] = []
    for i, operation in enumerate(ops):
        unknown = operation.status != OK
        call_entry = _Entry(operation, True, unknown)
        ret_entry = _Entry(operation, False, unknown)
        call_entry.match = ret_entry
        ret_entry.match = call_entry
        events.append((operation.call, 0, call_entry))
        ret_time = operation.ret if operation.ret is not None else horizon + i
        events.append((ret_time, 1, ret_entry))
    events.sort(key=lambda e: (e[0], e[1]))
    head = _Entry(None, False)
    prev = head
    for _, _, node in events:
        prev.next = node
        node.prev = prev
        prev = node

    def lift(call: _Entry) -> None:
        call.prev.next = call.next  # type: ignore[union-attr]
        if call.next is not None:
            call.next.prev = call.prev
        ret = call.match
        assert ret is not None
        ret.prev.next = ret.next  # type: ignore[union-attr]
        if ret.next is not None:
            ret.next.prev = ret.prev

    def unlift(call: _Entry) -> None:
        ret = call.match
        assert ret is not None
        ret.prev.next = ret  # type: ignore[union-attr]
        if ret.next is not None:
            ret.next.prev = ret
        call.prev.next = call  # type: ignore[union-attr]
        if call.next is not None:
            call.next.prev = call

    state: Any = model.init()
    linearized = 0
    stack: list[tuple[_Entry, Any, bool]] = []
    cache: set[tuple[int, Any]] = {(0, state)}
    best: list[Operation] = []
    best_state: Any = state
    stuck: Operation | None = None
    explored = 0
    entry: _Entry | None = head.next
    while head.next is not None:
        explored += 1
        if explored > budget:
            raise RuntimeError(
                f"linearizability search exceeded {budget} steps; record a shorter history "
                "or use a partitioned model"
            )
        assert entry is not None
        current = entry.op
        assert current is not None
        op: Operation = current
        bit = bit_of[op.index]
        if entry.is_call:
            legal, new_state = model.step(state, op)
            if legal:
                key = (linearized | bit, new_state)
                if key not in cache:
                    cache.add(key)
                    stack.append((entry, state, True))
                    state = new_state
                    linearized |= bit
                    lift(entry)
                    entry = head.next
                    continue
            entry = entry.next
            continue
        # A return entry: its operation must already have been linearized.
        if entry.unknown:
            key = (linearized | bit, state)
            if key not in cache:
                # Unknown outcome: it is allowed to never take effect.
                cache.add(key)
                match = entry.match
                assert match is not None
                stack.append((match, state, False))
                linearized |= bit
                lift(match)
                entry = head.next
                continue
        applied_now = [e.op for e, _, applied in stack if applied and e.op is not None]
        if stuck is None or len(applied_now) >= len(best):
            best = applied_now
            best_state = state
            stuck = op
        if not stack:
            return PartitionResult(False, list(operations), best, stuck, best_state, explored)
        popped, state, _ = stack.pop()
        assert popped.op is not None
        linearized &= ~bit_of[popped.op.index]
        unlift(popped)
        entry = popped.next
    linearization = [e.op for e, _, applied in stack if applied and e.op is not None]
    return PartitionResult(True, list(operations), linearization, None, state, explored)


def check(
    operations: Iterable[Operation], model: Model, *, budget: int = 5_000_000
) -> LinearizabilityResult:
    """Check a history against a sequential *model*."""
    ops = [op for op in operations if op.status != FAIL]
    results = []
    ok = True
    for part in model.partition(ops):
        result = _check_partition(model, part, budget)
        results.append(result)
        if not result.ok:
            ok = False
            break
    return LinearizabilityResult(ok, model, results)
