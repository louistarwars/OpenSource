from __future__ import annotations

import os

import pytest

pytest_plugins = ["pytester"]

# Never write the example database into the repository while testing detangle itself.
os.environ.setdefault("DETANGLE_DATABASE", "off")


@pytest.fixture(autouse=True)
def _no_forced_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "DETANGLE_RUNS",
        "DETANGLE_SEED",
        "DETANGLE_STRATEGY",
        "DETANGLE_REPLAY",
        "DETANGLE_MAX_DURATION",
        "DETANGLE_REPORT_DIR",
        "DETANGLE_SHRINK",
    ):
        monkeypatch.delenv(name, raising=False)
