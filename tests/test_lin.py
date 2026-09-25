"""The linearizability checker, validated against a brute-force oracle."""

from __future__ import annotations

import asyncio
import itertools
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import detangle
from detangle import BugFound, NotLinearizable
from detangle.lin import (
    KV,
    Counter,
    FIFOQueue,
    History,
    Model,
    Mutex,
    Operation,
    Register,
    Set,
    check,
)


def build(spec: list[tuple[Any, str, Any, Any, int, int | None]]) -> list[Operation]:
    """spec: (process, f, arg, output, call, ret) -- ret None = unknown outcome."""
    ops = []
    for i, (process, f, arg, output, call, ret) in enumerate(spec):
        op = Operation(i, process, f, arg, call, float(call))
        if ret is None:
            op.status = "info"
        else:
            op.status, op.output, op.ret, op.ret_time = "ok", output, ret, float(ret)
        ops.append(op)
    return ops


def brute_force(ops: list[Operation], model: Model) -> bool:
    known = [op for op in ops if op.status == "ok"]
    unknown = [op for op in ops if op.status != "ok"]
    for k in range(len(unknown) + 1):
        for chosen in itertools.combinations(unknown, k):
            candidates = known + list(chosen)
            for perm in itertools.permutations(candidates):
                position = {op.index: i for i, op in enumerate(perm)}
                if any(
                    a.ret is not None and a.ret < b.call and position[a.index] > position[b.index]
                    for a in candidates
                    for b in candidates
                ):
                    continue
                state = model.init()
                for op in perm:
                    ok, state = model.step(state, op)
                    if not ok:
                        break
                else:
                    return True
    return False


def test_simple_register() -> None:
    ops = build(
        [(1, "write", 1, None, 1, 2), (2, "read", None, 1, 3, 4), (3, "read", None, 0, 5, 6)]
    )
    result = check(ops, Register(0))
    assert not result.ok
    text = result.explain()
    assert "read() -> 0" in text
    assert "timeline" in text


def test_concurrent_read_may_see_either_value() -> None:
    for seen in (0, 1):
        ops = build([(1, "write", 1, None, 1, 4), (2, "read", None, seen, 2, 3)])
        assert check(ops, Register(0)).ok


def test_unknown_outcome_may_or_may_not_happen() -> None:
    happened = build([(1, "write", 1, None, 1, None), (2, "read", None, 1, 5, 6)])
    not_happened = build([(1, "write", 1, None, 1, None), (2, "read", None, 0, 5, 6)])
    assert check(happened, Register(0)).ok
    assert check(not_happened, Register(0)).ok


def test_cas_register() -> None:
    ops = build(
        [
            (1, "cas", (0, 1), True, 1, 2),
            (2, "cas", (0, 2), True, 3, 4),  # impossible: value is already 1
        ]
    )
    assert not check(ops, Register(0)).ok


def test_fifo_queue() -> None:
    good = build(
        [
            (1, "enqueue", "a", None, 1, 4),
            (2, "enqueue", "b", None, 2, 3),
            (3, "dequeue", None, "b", 5, 6),
        ]
    )
    bad = build(
        [
            (1, "enqueue", "a", None, 1, 2),
            (2, "enqueue", "b", None, 3, 4),
            (3, "dequeue", None, "b", 5, 6),
        ]
    )
    assert check(good, FIFOQueue()).ok
    assert not check(bad, FIFOQueue()).ok


def test_kv_is_checked_per_key() -> None:
    ops = build(
        [
            (1, "put", ("x", 1), None, 1, 2),
            (2, "put", ("y", 2), None, 3, 4),
            (1, "get", "x", 1, 5, 6),
            (2, "get", "y", 2, 7, 8),
            (3, "get", "z", None, 9, 10),
        ]
    )
    result = check(ops, KV())
    assert result.ok
    assert len(result.partitions) == 3
    stale = build([(1, "put", ("x", 1), None, 1, 2), (2, "get", "x", None, 3, 4)])
    assert not check(stale, KV()).ok


def test_counter_set_mutex() -> None:
    assert check(
        build([(1, "incr", 1, 1, 1, 2), (2, "incr", 1, 2, 3, 4), (3, "read", None, 2, 5, 6)]),
        Counter(),
    ).ok
    assert not check(build([(1, "incr", 1, 1, 1, 2), (2, "incr", 1, 1, 3, 4)]), Counter()).ok
    assert check(build([(1, "add", "a", True, 1, 2), (2, "contains", "a", True, 3, 4)]), Set()).ok
    assert not check(build([(1, "add", "a", True, 1, 2), (2, "add", "a", True, 3, 4)]), Set()).ok
    assert not check(
        build([(1, "acquire", None, None, 1, 2), (2, "acquire", None, None, 3, 4)]), Mutex()
    ).ok
    assert check(
        build(
            [
                (1, "acquire", None, None, 1, 2),
                (1, "release", None, None, 3, 4),
                (2, "acquire", None, None, 5, 6),
            ]
        ),
        Mutex(),
    ).ok


@st.composite
def histories(draw: st.DrawFn) -> list[Operation]:
    n = draw(st.integers(1, 6))
    # A random interleaving of 2n events; each op spans its two event slots.
    slots = draw(st.permutations([i for i in range(n) for _ in (0, 1)]))
    positions: dict[int, list[int]] = {}
    for t, i in enumerate(slots, 1):
        positions.setdefault(i, []).append(t)
    times = {(i, "call"): p[0] for i, p in positions.items()}
    times.update({(i, "ret"): p[1] for i, p in positions.items()})
    spec = []
    for i in range(n):
        f = draw(st.sampled_from(["read", "write", "cas"]))
        arg: Any = None
        output: Any = None
        if f == "write":
            arg = draw(st.integers(0, 2))
        elif f == "read":
            output = draw(st.integers(0, 2))
        else:
            arg = (draw(st.integers(0, 2)), draw(st.integers(0, 2)))
            output = draw(st.booleans())
        unknown = f != "read" and draw(st.booleans()) and draw(st.booleans())
        call = times[(i, "call")]
        ret = None if unknown else times[(i, "ret")]
        spec.append((i % 3, f, arg, output, call, ret))
    return build(spec)


@settings(max_examples=400, deadline=None)
@given(histories())
def test_checker_agrees_with_brute_force(ops: list[Operation]) -> None:
    assert check(ops, Register(0)).ok == brute_force(ops, Register(0))


def test_history_recorder_and_explore_integration() -> None:
    class BrokenCache:
        """A read-through cache that can serve stale data (not linearizable)."""

        def __init__(self) -> None:
            self.db: dict[str, int] = {}
            self.cache: dict[str, int] = {}

        async def put(self, key: str, value: int) -> None:
            await asyncio.sleep(0.001)
            self.db[key] = value
            await asyncio.sleep(0.001)
            self.cache.pop(key, None)  # invalidate

        async def get(self, key: str) -> int | None:
            if key in self.cache:
                return self.cache[key]
            value = self.db.get(key)
            await asyncio.sleep(0.001)
            self.cache[key] = value  # may cache a value that is already stale
            return value

    async def main() -> None:
        kv = BrokenCache()
        history = History()

        async def writer() -> None:
            for v in (1, 2):
                await history.call("w", "put", ("k", v), kv.put("k", v))

        async def reader(name: str) -> None:
            for _ in range(3):
                await history.call(name, "get", "k", kv.get("k"))
                await asyncio.sleep(0.002)

        await asyncio.gather(writer(), reader("r1"), reader("r2"))
        history.assert_linearizable(KV())

    with pytest.raises(BugFound) as info:
        detangle.explore(main, runs=300, seed=1)
    assert isinstance(info.value.report.failure.exception, NotLinearizable)
    assert "not linearizable" in str(info.value)


def test_history_call_records_unknown_on_exception() -> None:
    async def main() -> str:
        history = History()

        async def fails() -> None:
            raise TimeoutError

        with pytest.raises(TimeoutError):
            await history.call(1, "write", 5, fails())
        with pytest.raises(KeyError):
            await history.call(1, "write", 6, _raise(KeyError()), fail_on=(KeyError,))
        return ",".join(op.status for op in history.operations)

    assert detangle.run(main) == "info,fail"


async def _raise(exc: BaseException) -> None:
    raise exc


def test_double_completion_is_an_error() -> None:
    history = History()
    op = history.invoke(1, "read")
    history.ok(op, 1)
    with pytest.raises(ValueError):
        history.ok(op, 2)


class BankAccount(Model):
    """The custom model from docs/linearizability.md."""

    name = "account"

    def init(self) -> int:
        return 0

    def step(self, state: Any, op: Operation) -> tuple[bool, Any]:
        if op.f == "deposit":
            return True, state + op.arg
        if op.f == "withdraw":
            if op.status == "ok" and op.output is False:
                return True, state
            return (state >= op.arg), state - op.arg
        if op.f == "balance":
            return (op.status != "ok" or op.output == state), state
        raise ValueError(op.f)


def test_custom_model_from_docs() -> None:
    ok = build(
        [
            (1, "deposit", 10, None, 1, 2),
            (2, "withdraw", 5, True, 3, 4),
            (1, "balance", None, 5, 5, 6),
        ]
    )
    bad = build([(1, "deposit", 10, None, 1, 2), (2, "withdraw", 50, True, 3, 4)])
    assert check(ok, BankAccount()).ok
    assert not check(bad, BankAccount()).ok
