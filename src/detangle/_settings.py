"""Run-time settings: environment variables and command-line overrides.

Precedence (highest first): values set by the pytest plugin / CLI,
``DETANGLE_*`` environment variables, arguments given in code, defaults.
Command-line and environment values are explicit requests made when
*invoking* the tests ("run 10x more", "replay this token"), so they win.
"""

from __future__ import annotations

import os
from typing import Any

__all__ = ["get", "overrides", "reset_overrides"]

_ENV = {
    "runs": "DETANGLE_RUNS",
    "strategy": "DETANGLE_STRATEGY",
    "seed": "DETANGLE_SEED",
    "max_duration": "DETANGLE_MAX_DURATION",
    "replay": "DETANGLE_REPLAY",
    "database": "DETANGLE_DATABASE",
    "report_dir": "DETANGLE_REPORT_DIR",
    "shrink": "DETANGLE_SHRINK",
    "verbose": "DETANGLE_VERBOSE",
}

DEFAULTS: dict[str, Any] = {
    "runs": 200,
    "strategy": "auto",
    "seed": None,
    "max_duration": None,
    "replay": None,
    "database": ".detangle",
    "report_dir": None,
    "shrink": True,
    "verbose": False,
}

_overrides: dict[str, Any] = {}


def overrides(**values: Any) -> None:
    """Set process-wide overrides (used by the pytest plugin)."""
    for key, value in values.items():
        if key not in DEFAULTS:
            raise KeyError(key)
        if value is not None:
            _overrides[key] = value


def reset_overrides() -> None:
    _overrides.clear()


def _parse(key: str, raw: str) -> Any:
    raw = raw.strip()
    if key in ("runs",):
        return int(raw)
    if key == "seed":
        return int(raw, 0)
    if key == "max_duration":
        return float(raw)
    if key in ("shrink", "verbose"):
        return raw.lower() not in ("0", "false", "no", "off", "")
    if key == "database" and raw.lower() in ("0", "false", "no", "off", "none", ""):
        return False
    return raw


def forced(key: str) -> tuple[bool, Any]:
    """(True, value) when the CLI or the environment forces this setting."""
    if key in _overrides:
        return True, _overrides[key]
    env = os.environ.get(_ENV[key])
    if env is not None and env.strip() != "":
        return True, _parse(key, env)
    return False, None


def get(key: str, explicit: Any = None) -> Any:
    is_forced, value = forced(key)
    if is_forced:
        return value
    if explicit is not None:
        return explicit
    return DEFAULTS[key]
