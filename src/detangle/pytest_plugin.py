"""pytest integration (registered automatically through the ``pytest11`` entry point).

* ``@pytest.mark.detangle(runs=..., strategy=..., ...)`` on an ``async def``
  test runs it under detangle (no decorator import needed);
* ``@detangle.test`` works as-is;
* command-line options override the settings of every detangle test::

    pytest --detangle-runs=5000 --detangle-seed=42
    pytest --detangle-replay=dt1-... tests/test_bank.py::test_transfer
    pytest --detangle-report-dir=detangle-reports   # interactive HTML reports
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from . import _settings
from ._version import __version__
from .explore import test as detangle_test


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("detangle", "detangle: deterministic simulation testing for asyncio")
    group.addoption(
        "--detangle-runs", type=int, default=None, help="number of schedules to explore per test"
    )
    group.addoption(
        "--detangle-strategy",
        default=None,
        help="auto, pct[:depth], random, dfs[:max_delays], fifo",
    )
    group.addoption(
        "--detangle-seed", default=None, help="seed for the exploration (reproducible runs)"
    )
    group.addoption(
        "--detangle-replay", default=None, help="replay this token instead of exploring"
    )
    group.addoption(
        "--detangle-max-duration",
        type=float,
        default=None,
        help="max seconds of exploration per test",
    )
    group.addoption(
        "--detangle-report-dir",
        default=None,
        help="write an interactive HTML report for every bug found",
    )
    group.addoption(
        "--detangle-no-db",
        action="store_true",
        default=False,
        help="do not read/write the .detangle example database",
    )
    group.addoption(
        "--detangle-no-shrink",
        action="store_true",
        default=False,
        help="report failures without shrinking them",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "detangle(**options): run this async test under detangle's deterministic simulation "
        "(options: runs, strategy, seed, max_duration, timer_jitter, net, ...)",
    )
    seed = config.getoption("--detangle-seed", None)
    _settings.overrides(
        runs=config.getoption("--detangle-runs", None),
        strategy=config.getoption("--detangle-strategy", None),
        seed=int(seed, 0) if isinstance(seed, str) else seed,
        replay=config.getoption("--detangle-replay", None),
        max_duration=config.getoption("--detangle-max-duration", None),
        report_dir=config.getoption("--detangle-report-dir", None),
    )
    if config.getoption("--detangle-no-db", False):
        _settings.overrides(database=False)
    if config.getoption("--detangle-no-shrink", False):
        _settings.overrides(shrink=False)


def pytest_unconfigure(config: pytest.Config) -> None:
    _settings.reset_overrides()


def pytest_report_header(config: pytest.Config) -> str:
    runs = _settings.get("runs")
    strategy = _settings.get("strategy")
    return f"detangle {__version__} (runs={runs}, strategy={strategy})"


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> Any:
    marker = pyfuncitem.get_closest_marker("detangle")
    if marker is None:
        return None
    func = pyfuncitem.obj
    if getattr(func, "_detangle_inner", None) is not None:
        return None  # already decorated with @detangle.test
    if not inspect.iscoroutinefunction(func):
        raise pytest.UsageError(
            f"@pytest.mark.detangle can only be used on async def tests ({pyfuncitem.nodeid})"
        )
    if marker.args:
        raise pytest.UsageError("@pytest.mark.detangle takes keyword arguments only")
    decorate: Any = detangle_test(**marker.kwargs)
    wrapped = decorate(func)
    argnames = pyfuncitem._fixtureinfo.argnames
    wrapped(**{name: pyfuncitem.funcargs[name] for name in argnames})
    return True
