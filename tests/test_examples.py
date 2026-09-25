"""The examples are part of the test suite: they must keep finding their bugs."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def load(name: str):  # type: ignore[no-untyped-def]
    path = EXAMPLES / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"example_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("bank_race", "overdraft"),
        ("dining_philosophers", "wait-for cycle"),
        ("order_events", "shipped before it was created"),
        ("connection_pool", "leaked"),
        ("cache_stampede", "at most one backend call in flight"),
        ("line_protocol", "fragmented"),
        ("replicated_kv", "not linearizable"),
    ],
)
def test_example_finds_its_bug_and_fix_passes(name: str, expected: str) -> None:
    found, check = load(name).demo()
    assert "detangle found a bug" in found
    assert expected in found
    assert "no bug found" in check or "all schedules within the bound" in check


def test_retry_backoff_example() -> None:
    assert "hours of virtual time" in load("retry_backoff").demo()
