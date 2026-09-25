"""pytest plugin, command line, tokens and strategies."""

from __future__ import annotations

import asyncio
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

import detangle
from detangle import (
    DFS,
    FIFO,
    PCT,
    Decision,
    Portfolio,
    RandomWalk,
    Replay,
    decode_token,
    encode_token,
)
from detangle.__main__ import main as cli_main
from detangle.strategies import make_strategy

RACY_TEST = textwrap.dedent(
    """
    import asyncio
    import detangle
    import pytest

    async def _writes():
        log = []
        async def w(name):
            await asyncio.sleep(0.01)
            log.append(name)
        await asyncio.gather(w("a"), w("b"))
        assert log == ["a", "b"], log

    @pytest.mark.detangle(runs=50, seed=1)
    async def test_marked():
        await _writes()

    @detangle.test(runs=50, seed=1)
    async def test_decorated():
        await _writes()

    @pytest.mark.detangle(runs=5)
    async def test_fine(tmp_path):
        assert tmp_path.exists()
        await asyncio.sleep(10)
    """
)


@given(st.lists(st.integers(0, 2**40), max_size=200))
def test_token_roundtrip(values: list[int]) -> None:
    decoded = decode_token(encode_token(values))
    while values and values[-1] == 0:
        values = values[:-1]
    assert decoded == values


def test_token_is_compact_for_mostly_default_schedules() -> None:
    values = [0] * 5000 + [3] + [0] * 5000 + [1]
    token = encode_token(values)
    assert len(token) < 60
    assert decode_token(token) == values


@pytest.mark.parametrize("bad", ["", "dt2-AAAA", "dt1-!!!!", "dt1-", "dt1-eA"])
def test_bad_tokens_are_rejected(bad: str) -> None:
    with pytest.raises(ValueError):
        decode_token(bad)


class _L:
    def __init__(self, i: int) -> None:
        self.id = i


def test_strategy_factory() -> None:
    assert isinstance(make_strategy("auto", 1), Portfolio)
    assert isinstance(make_strategy("pct:4", 1), PCT)
    assert make_strategy("pct:4", 1).depth == 4  # type: ignore[attr-defined]
    assert isinstance(make_strategy("random", 1), RandomWalk)
    dfs = make_strategy("dfs:5")
    assert isinstance(dfs, DFS) and dfs.max_delays == 5
    assert isinstance(make_strategy("fifo"), FIFO)
    with pytest.raises(ValueError):
        make_strategy("nope")


def test_replay_strategy_clamps_and_defaults() -> None:
    r = Replay([5, 1])
    r.start(0)
    assert r.schedule([_L(1), _L(2)]) == 1  # clamped to n-1
    assert r.choose(10, "data") == 1
    assert r.choose(10, "data") == 0  # past the end: default
    assert r.flip(0.5, "fault") is False


def test_pct_prefers_highest_priority_and_is_deterministic() -> None:
    lanes = [_L(1), _L(2), _L(3)]
    picks = []
    for _ in range(2):
        p = PCT(seed=42, depth=1)
        p.start(0)
        picks.append([p.schedule(lanes) for _ in range(5)])
    assert picks[0] == picks[1]
    assert len(set(picks[0])) == 1  # depth 1: no priority change points


def test_dfs_enumerates_bounded_sequences() -> None:
    dfs = DFS(max_delays=1)
    seen = []
    while not dfs.exhausted:
        dfs.start(0)
        a = dfs.choose(3, "data")
        b = dfs.choose(2, "data")
        seen.append((a, b))
        dfs.finish([Decision("data", 3, a), Decision("data", 2, b)], failed=False)
    assert seen == [(0, 0), (0, 1), (1, 0)]


def test_pytest_plugin_marker_and_decorator(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_racy=RACY_TEST)
    result = pytester.runpytest("--detangle-no-db")
    result.assert_outcomes(passed=1, failed=2)
    result.stdout.fnmatch_lines(["*detangle found a bug in*", "*DETANGLE_REPLAY=dt1-*"])


def test_pytest_plugin_cli_overrides(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        test_count=textwrap.dedent(
            """
            import detangle
            RUNS = []

            @detangle.test(runs=3)
            async def test_counting():
                RUNS.append(1)

            def test_check():
                assert len(RUNS) == 11
            """
        )
    )
    result = pytester.runpytest("--detangle-runs=11", "--detangle-no-db")
    result.assert_outcomes(passed=2)


def test_pytest_plugin_replay_option(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_racy=RACY_TEST)
    first = pytester.runpytest("--detangle-no-db", "-k", "test_decorated")
    token = next(
        line.split("DETANGLE_REPLAY=")[1].split()[0]
        for line in first.stdout.lines
        if "DETANGLE_REPLAY=" in line
    )
    again = pytester.runpytest(
        "--detangle-no-db", "-k", "test_decorated", f"--detangle-replay={token}"
    )
    again.assert_outcomes(failed=1)
    again.stdout.fnmatch_lines(["*replaying*reproduces a bug*"])


def test_pytest_report_header(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_x="def test_x(): pass")
    result = pytester.runpytest()
    result.stdout.fnmatch_lines(["detangle * (runs=200, strategy=auto)"])


def _write_target(tmp_path: Path) -> Path:
    path = tmp_path / "target_mod.py"
    path.write_text(
        textwrap.dedent(
            """
            import asyncio

            async def racy():
                log = []
                async def w(name):
                    await asyncio.sleep(0.01)
                    log.append(name)
                await asyncio.gather(w("a"), w("b"))
                assert log == ["a", "b"], log

            async def fine():
                await asyncio.sleep(1)
            """
        )
    )
    return path


def test_cli_explore_and_replay(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    target = _write_target(tmp_path)
    assert cli_main(["explore", f"{target}:fine", "--runs", "5", "--no-db"]) == 0
    assert "5 runs" in capsys.readouterr().out
    assert cli_main(["explore", f"{target}:racy", "--runs", "50", "--seed", "1", "--no-db"]) == 1
    out = capsys.readouterr().out
    token = out.split('detangle.replay(racy, "')[1].split('"')[0]
    html = tmp_path / "report.html"
    assert cli_main(["replay", token, f"{target}:racy", "--html", str(html)]) == 1
    assert html.exists() and token in html.read_text()
    assert cli_main(["replay", token, f"{target}:racy", "--json"]) == 1
    assert '"kind": "exception"' in capsys.readouterr().out
    assert cli_main(["decode", token]) == 0
    assert "non-default" in capsys.readouterr().out


def test_module_entry_point() -> None:
    out = subprocess.run(
        [sys.executable, "-m", "detangle", "--version"], capture_output=True, text=True, check=True
    )
    assert out.stdout.startswith("detangle ")


def test_public_api_is_complete() -> None:
    for name in detangle.__all__:
        assert hasattr(detangle, name), name
    assert detangle.__version__


def test_loop_can_be_used_directly() -> None:
    loop = detangle.SimLoop(strategy=RandomWalk(seed=3))

    async def main() -> float:
        await asyncio.sleep(5)
        return loop.time()

    assert loop.run_until_complete(main()) == 5
    loop.close()
