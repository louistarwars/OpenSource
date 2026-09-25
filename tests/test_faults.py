"""Deadlocks, invariants and injected cancellations."""

from __future__ import annotations

import asyncio

import pytest

import detangle
from detangle import BugFound


async def dining(n: int = 3) -> None:
    forks = [asyncio.Lock() for _ in range(n)]

    async def philosopher(i: int) -> None:
        left, right = forks[i], forks[(i + 1) % n]
        async with left:
            await asyncio.sleep(0.01)  # think while holding one fork
            async with right:
                await asyncio.sleep(0.01)

    await asyncio.gather(*(philosopher(i) for i in range(n)))


def test_deadlock_report_names_locks_holders_and_cycle() -> None:
    with pytest.raises(BugFound) as info:
        detangle.explore(dining, runs=20, seed=1)
    report = info.value.report
    assert report.failure.kind == "deadlock"
    text = str(info.value)
    assert "Deadlock" in text
    assert "waits to acquire Lock `right`" in text
    assert "held by" in text
    assert "wait-for cycle" in text
    deadlock = report.failure.deadlock
    assert deadlock is not None
    assert len(deadlock.cycles) == 1
    assert len(deadlock.cycles[0]) == 3


def test_deadlock_error_from_run() -> None:
    with pytest.raises(detangle.DeadlockError) as info:
        detangle.run(dining)
    assert "wait-for cycle" in str(info.value)


def test_lost_wakeup_is_explained() -> None:
    async def main() -> None:
        ready = asyncio.Event()
        items: asyncio.Queue[int] = asyncio.Queue()

        async def consumer() -> None:
            await items.get()

        async def waiter() -> None:
            await ready.wait()

        await asyncio.gather(consumer(), waiter())

    with pytest.raises(detangle.DeadlockError) as info:
        detangle.run(main)
    text = str(info.value)
    assert "Event `ready` to be set" in text
    assert "item from Queue `items`" in text


def test_deadlock_on_awaiting_task() -> None:
    async def main() -> None:
        never: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        async def inner() -> None:
            await never

        await asyncio.create_task(inner(), name="inner")

    with pytest.raises(detangle.DeadlockError) as info:
        detangle.run(main)
    assert "for task: inner" in str(info.value)


def test_semaphore_holders_are_tracked() -> None:
    async def main() -> None:
        sem = asyncio.Semaphore(1)
        gate = asyncio.Event()

        async def holder() -> None:
            async with sem:
                await gate.wait()

        async def blocked() -> None:
            await asyncio.sleep(0.1)
            async with sem:
                pass

        await asyncio.gather(holder(), blocked())

    with pytest.raises(detangle.DeadlockError) as info:
        detangle.run(main)
    assert "Semaphore `sem`, held by holder#2" in str(info.value)


def test_invariant_checked_after_every_step() -> None:
    async def main() -> None:
        account = {"a": 50, "b": 50}
        detangle.invariant(lambda: account["a"] + account["b"] == 100, "money is conserved")

        async def transfer(src: str, dst: str, amount: int) -> None:
            account[src] -= amount
            await asyncio.sleep(0.001)  # transient inconsistency visible to others
            account[dst] += amount

        await asyncio.gather(transfer("a", "b", 10), transfer("b", "a", 5))

    with pytest.raises(BugFound) as info:
        detangle.explore(main, runs=5)
    assert info.value.report.failure.kind == "invariant"
    assert "money is conserved" in str(info.value)


def test_invariant_with_assert_message() -> None:
    async def main() -> None:
        state = {"n": 0}

        def check() -> None:
            assert state["n"] < 3, f"n reached {state['n']}"

        detangle.invariant(check)
        for _ in range(5):
            state["n"] += 1
            await asyncio.sleep(0)

    with pytest.raises(BugFound) as info:
        detangle.explore(main, runs=1)
    assert "n reached 3" in str(info.value)


class Pool:
    """A connection pool that is NOT cancellation-safe."""

    def __init__(self, size: int) -> None:
        self.free = size
        self.cond = asyncio.Condition()

    async def acquire(self) -> None:
        async with self.cond:
            await self.cond.wait_for(lambda: self.free > 0)
            self.free -= 1

    async def release(self) -> None:
        async with self.cond:
            self.free += 1
            self.cond.notify()

    async def query(self) -> str:
        await self.acquire()
        await asyncio.sleep(0.01)  # talk to the database
        await self.release()  # BUG: skipped if cancelled while querying
        return "row"

    async def safe_query(self) -> str:
        await self.acquire()
        try:
            await asyncio.sleep(0.01)
            return "row"
        finally:
            await asyncio.shield(self.release())


def test_maybe_timeout_finds_cancellation_bug() -> None:
    async def main() -> None:
        pool = Pool(2)

        async def client() -> None:
            try:
                await detangle.maybe_timeout(pool.query())
            except TimeoutError:
                pass

        await asyncio.gather(*(client() for _ in range(3)))
        assert pool.free == 2, f"connections leaked: {2 - pool.free}"

    with pytest.raises(BugFound) as info:
        detangle.explore(main, runs=200, seed=1)
    assert "connections leaked" in str(info.value)
    assert "injected cancellation" in str(info.value)


def test_maybe_timeout_passes_on_safe_code() -> None:
    async def main() -> None:
        pool = Pool(2)

        async def client() -> None:
            try:
                await detangle.maybe_timeout(pool.safe_query())
            except TimeoutError:
                pass

        await asyncio.gather(*(client() for _ in range(3)))
        await asyncio.sleep(1)
        assert pool.free == 2

    stats = detangle.explore(main, runs=200, seed=1)
    assert stats.runs == 200


def test_maybe_timeout_returns_result_by_default() -> None:
    async def main() -> int:
        async def compute() -> int:
            await asyncio.sleep(1)
            return 7

        return await detangle.maybe_timeout(compute())

    assert detangle.run(main) == 7


def test_inject_cancellation_marks_task() -> None:
    async def main() -> None:
        async def worker() -> None:
            for _ in range(5):
                await asyncio.sleep(0.1)

        task = asyncio.create_task(worker())
        detangle.inject_cancellation(task, points=3)
        await task

    with pytest.raises(BugFound) as info:
        detangle.explore(main, runs=50, seed=2)
    assert isinstance(info.value.report.failure.exception, asyncio.CancelledError)


def test_mailbox_wait_is_explained() -> None:
    async def main() -> None:
        inbox = detangle.network().mailbox(9, host="n1")
        await inbox.recv()

    with pytest.raises(detangle.DeadlockError) as info:
        detangle.run(main)
    assert "for a message on <Mailbox n1:9>" in str(info.value)


def test_cycle_search_is_linear_and_bounded() -> None:
    from detangle._deadlock import _find_cycles

    assert _find_cycles({"a": ["b"], "b": ["a"], "c": ["d"], "d": ["c"], "e": ["a"]}) == [
        ["a", "b"],
        ["c", "d"],
    ]
    # A dense graph must not blow up.
    nodes = [f"t{i}" for i in range(200)]
    dense = {n: [m for m in nodes if m != n] for n in nodes}
    assert 1 <= len(_find_cycles(dense)) <= 5
