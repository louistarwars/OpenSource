"""Finding, shrinking and replaying bugs."""

from __future__ import annotations

import asyncio

import pytest

import detangle
from detangle import DFS, PCT, BugFound, RandomWalk


async def lost_update() -> None:
    """Two tasks woken by the same timer tick race on a read-modify-write."""
    state = {"balance": 100}

    async def withdraw(amount: int) -> None:
        await asyncio.sleep(0.01)  # e.g. waiting for a request
        current = state["balance"]
        await asyncio.sleep(0)  # e.g. an audit-log write
        state["balance"] = current - amount

    await asyncio.gather(withdraw(30), withdraw(50))
    assert state["balance"] == 20, f"lost update: balance is {state['balance']}"


async def ordered_writes() -> None:
    """Passes under asyncio's FIFO order, fails under a different wake-up order."""
    log: list[str] = []

    async def writer(name: str) -> None:
        await asyncio.sleep(0.01)
        log.append(name)

    await asyncio.gather(writer("first"), writer("second"))
    assert log == ["first", "second"], log


def test_fifo_run_passes_but_exploration_finds_the_race() -> None:
    detangle.run(ordered_writes)  # asyncio's default order: fine
    with pytest.raises(BugFound) as info:
        detangle.explore(ordered_writes, runs=50, seed=1)
    report = info.value.report
    assert report.failure.kind == "exception"
    assert isinstance(report.failure.exception, AssertionError)
    assert report.deviations == 1
    assert "ran ahead of" in str(info.value)
    assert report.token.startswith("dt1-")


def test_bug_found_is_an_assertion_error_chained_to_the_original() -> None:
    with pytest.raises(AssertionError) as info:
        detangle.explore(lost_update, runs=100, seed=3)
    assert isinstance(info.value, BugFound)
    assert isinstance(info.value.__cause__, AssertionError)
    assert "lost update" in str(info.value)


def test_replay_reproduces_exactly() -> None:
    with pytest.raises(BugFound) as info:
        detangle.explore(ordered_writes, runs=50, seed=7)
    token = info.value.token
    for _ in range(3):
        with pytest.raises(BugFound) as again:
            detangle.replay(ordered_writes, token)
        assert again.value.report.failure.headline() == info.value.report.failure.headline()
    result = detangle.replay(ordered_writes, token, raise_on_failure=False)
    assert result.failure is not None
    assert result.trace is not None and result.trace.events


def test_run_with_replay_token_raises_original_exception() -> None:
    with pytest.raises(BugFound) as info:
        detangle.explore(ordered_writes, runs=50, seed=2)
    with pytest.raises(AssertionError, match="second"):
        detangle.run(ordered_writes, replay=info.value.token)


def test_same_seed_same_exploration() -> None:
    def find(seed: int) -> str:
        with pytest.raises(BugFound) as info:
            detangle.explore(lost_update, runs=200, seed=seed, strategy="random", shrink=False)
        return info.value.token

    assert find(11) == find(11)


def test_shrinking_reduces_deviations() -> None:
    async def needle() -> None:
        # Lots of noise, but the bug needs a single inversion.
        log: list[int] = []

        async def worker(i: int) -> None:
            for _ in range(3):
                await asyncio.sleep(0)
            await asyncio.sleep(0.01)
            log.append(i)

        await asyncio.gather(*(worker(i) for i in range(6)))
        assert log.index(0) < log.index(5), log

    with pytest.raises(BugFound) as info:
        detangle.explore(needle, runs=300, seed=5, strategy="random")
    report = info.value.report
    assert report.original_deviations is not None
    assert report.deviations <= 2
    assert report.deviations <= report.original_deviations


def test_dfs_is_exhaustive() -> None:
    orders: set[tuple[str, ...]] = set()

    async def three_way() -> None:
        log: list[str] = []

        async def t(name: str) -> None:
            await asyncio.sleep(0.01)
            log.append(name)

        await asyncio.gather(t("a"), t("b"), t("c"))
        orders.add(tuple(log))

    stats = detangle.explore(three_way, runs=100_000, strategy=DFS(max_delays=10))
    assert stats.exhausted
    # Three tasks woken by the same tick can finish in any of the 3! orders.
    assert len(orders) == 6
    assert stats.distinct_schedules <= stats.runs


def test_dfs_finds_bug_with_bounded_delays() -> None:
    with pytest.raises(BugFound) as info:
        detangle.explore(ordered_writes, runs=1000, strategy="dfs:1")
    assert info.value.report.deviations == 1


def test_dfs_proves_absence_within_bound() -> None:
    async def safe() -> None:
        lock = asyncio.Lock()
        state = {"n": 0}

        async def incr() -> None:
            await asyncio.sleep(0.01)
            async with lock:
                current = state["n"]
                await asyncio.sleep(0)
                state["n"] = current + 1

        await asyncio.gather(incr(), incr(), incr())
        assert state["n"] == 3

    stats = detangle.explore(safe, runs=100_000, strategy="dfs:3")
    assert stats.exhausted
    assert stats.runs > 10


def test_pct_and_random_strategies_find_the_race() -> None:
    for strategy in (PCT(seed=1, depth=2), RandomWalk(seed=1)):
        with pytest.raises(BugFound):
            detangle.explore(lost_update, runs=200, strategy=strategy)


def test_strict_fifo_mode_does_not_reorder() -> None:
    stats = detangle.explore(ordered_writes, runs=50, seed=1, reorder=False)
    assert stats.distinct_schedules == 1


def test_timer_jitter_explores_time_races() -> None:
    async def race() -> None:
        log: list[str] = []

        async def fast() -> None:
            await asyncio.sleep(0.010)
            log.append("fast")

        async def slow() -> None:
            await asyncio.sleep(0.012)
            log.append("slow")

        await asyncio.gather(fast(), slow())
        assert log == ["fast", "slow"]

    # Without jitter (and in strict FIFO order) the fast timer always wins.
    detangle.explore(race, runs=50, seed=1, reorder=False)
    with pytest.raises(BugFound):
        detangle.explore(race, runs=200, seed=1, reorder=False, timer_jitter=0.005)


def test_decorator_runs_exploration() -> None:
    calls = []

    @detangle.test(runs=17, seed=1)
    async def test_fine() -> None:
        calls.append(1)
        await asyncio.sleep(0)

    test_fine()
    assert len(calls) == 17

    @detangle.test(runs=100, seed=1)
    async def test_racy() -> None:
        await ordered_writes()

    with pytest.raises(BugFound):
        test_racy()


def test_decorator_forwards_arguments() -> None:
    seen = []

    @detangle.test(runs=3)
    async def test_with_args(x: int, y: str = "") -> None:
        seen.append((x, y))

    test_with_args(1, y="a")
    assert seen == [(1, "a")] * 3


def test_decorator_rejects_sync_functions() -> None:
    with pytest.raises(TypeError):
        detangle.test(lambda: None)  # type: ignore[arg-type]


def test_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    count = []

    async def fn() -> None:
        count.append(1)

    monkeypatch.setenv("DETANGLE_RUNS", "7")
    stats = detangle.explore(fn, runs=100)
    assert stats.runs == 7
    assert len(count) == 7


def test_env_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(BugFound) as info:
        detangle.explore(ordered_writes, runs=50, seed=1)
    monkeypatch.setenv("DETANGLE_REPLAY", info.value.token)
    with pytest.raises(BugFound) as again:
        detangle.explore(ordered_writes, runs=1)
    assert again.value.report.source == "replay"


def test_example_database_replays_failures_first(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DETANGLE_DATABASE", raising=False)
    db = str(tmp_path / "db")
    with pytest.raises(BugFound) as first:
        detangle.explore(ordered_writes, runs=100, seed=1, database=db, name="t")
    # Even with an exploration budget that cannot find it, the saved example fails first.
    with pytest.raises(BugFound) as second:
        detangle.explore(ordered_writes, runs=1, strategy="fifo", database=db, name="t")
    assert second.value.report.source == "database"
    assert second.value.token == first.value.token

    # Once the bug is fixed, the example is discarded.
    async def fixed() -> None:
        pass

    detangle.explore(fixed, runs=1, database=db, name="t")
    assert not [p for p in (tmp_path / "db" / "examples").rglob("*") if p.is_file()]


def test_unobserved_background_exception_is_a_bug() -> None:
    async def main() -> None:
        async def crash() -> None:
            raise RuntimeError("background failure")

        asyncio.create_task(crash())
        await asyncio.sleep(1)

    with pytest.raises(BugFound) as info:
        detangle.explore(main, runs=5)
    assert info.value.report.failure.kind == "unobserved-exception"
    assert "background failure" in str(info.value)

    stats = detangle.explore(main, runs=5, fail_on_unobserved=False)
    assert stats.runs == 5


def test_callback_exception_is_a_bug() -> None:
    async def main() -> None:
        asyncio.get_running_loop().call_soon(lambda: 1 / 0)
        await asyncio.sleep(0.1)

    with pytest.raises(BugFound) as info:
        detangle.explore(main, runs=2)
    assert info.value.report.failure.kind == "callback-error"


def test_step_limit_detects_livelock() -> None:
    async def spin() -> None:
        flag = {"done": False}

        async def waiter() -> None:
            while not flag["done"]:
                await asyncio.sleep(0)

        await waiter()

    with pytest.raises(BugFound) as info:
        detangle.explore(spin, runs=1, max_steps=500)
    assert info.value.report.failure.kind == "step-limit"


def test_time_limit_detects_runaway_retries() -> None:
    async def retry_forever() -> None:
        while True:
            await asyncio.sleep(60)

    with pytest.raises(BugFound) as info:
        detangle.explore(retry_forever, runs=1, max_time=3600)
    assert info.value.report.failure.kind == "time-limit"


def test_check_leaks() -> None:
    async def leaky() -> None:
        asyncio.create_task(asyncio.sleep(100))

    detangle.explore(leaky, runs=1)
    with pytest.raises(BugFound) as info:
        detangle.explore(leaky, runs=1, check_leaks=True)
    assert info.value.report.failure.kind == "task-leak"


def test_data_choices_are_explored_and_shrunk() -> None:
    async def main() -> None:
        a = detangle.randint(0, 50)
        b = detangle.choice(["x", "y", "z"])
        c = detangle.uniform(0.0, 1.0)
        assert not (a >= 10 and b == "z"), (a, b, c)

    with pytest.raises(BugFound) as info:
        detangle.explore(main, runs=500, seed=4, strategy="random")
    exc = info.value.report.failure.exception
    assert exc is not None
    assert str(exc).startswith("(10, 'z', 0.0)")  # shrunk to the boundary


def test_shuffle_default_is_identity() -> None:
    async def main() -> list[int]:
        items = list(range(6))
        detangle.shuffle(items)
        return items

    assert detangle.run(main) == list(range(6))
    shuffled = {tuple(detangle.run(main, seed=s)) for s in range(20)}
    assert len(shuffled) > 1
    assert all(sorted(s) == list(range(6)) for s in shuffled)


def test_note_appears_in_trace() -> None:
    async def main() -> None:
        detangle.note("checkpoint reached")
        await ordered_writes()

    with pytest.raises(BugFound) as info:
        detangle.explore(main, runs=50, seed=1)
    assert "checkpoint reached" in str(info.value)


def test_in_simulation_helpers() -> None:
    assert not detangle.in_simulation()

    async def main() -> tuple[bool, float]:
        await asyncio.sleep(2)
        return detangle.in_simulation(), detangle.now()

    assert detangle.run(main) == (True, 2.0)
    with pytest.raises(RuntimeError, match="inside a detangle simulation"):
        detangle.now()


def test_stats_summary() -> None:
    async def fine() -> None:
        await asyncio.gather(asyncio.sleep(0.1), asyncio.sleep(0.1))

    stats = detangle.explore(fine, runs=20, seed=1)
    assert stats.runs == 20
    assert "20 runs" in stats.summary()
    assert stats.max_virtual_time == pytest.approx(0.1)


def test_html_report(tmp_path) -> None:
    with pytest.raises(BugFound) as info:
        detangle.explore(ordered_writes, runs=50, seed=1, report_dir=str(tmp_path))
    path = info.value.report.html_report
    assert path is not None
    html = next(tmp_path.glob("*.html")).read_text()
    assert info.value.token in html
    assert "<script" in html and "ordered_writes" in html
