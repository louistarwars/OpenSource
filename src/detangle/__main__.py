"""Command-line interface.

Usage::

    detangle explore tests/test_bank.py:test_transfer --runs 2000
    detangle replay dt1-eJxj... tests/test_bank.py:test_transfer --html report.html
    detangle decode dt1-eJxj...
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import os
import sys
from pathlib import Path
from typing import Any

from ._choices import decode_token
from ._version import __version__


def _load(target: str) -> Any:
    if ":" not in target:
        raise SystemExit(
            f"error: target must look like 'module:function' or 'path.py:function', got {target!r}"
        )
    module_part, _, attr = target.rpartition(":")
    if module_part.endswith(".py") or os.sep in module_part or "/" in module_part:
        path = Path(module_part).resolve()
        if not path.exists():
            raise SystemExit(f"error: no such file: {module_part}")
        sys.path.insert(0, str(path.parent))
        name = path.stem
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise SystemExit(f"error: cannot import {module_part}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    else:
        sys.path.insert(0, os.getcwd())
        module = importlib.import_module(module_part)
    obj: Any = module
    for part in attr.split("."):
        obj = getattr(obj, part)
    return getattr(obj, "_detangle_inner", obj)


def _cmd_explore(args: argparse.Namespace) -> int:
    from .errors import BugFound
    from .explore import explore

    fn = _load(args.target)
    try:
        stats = explore(
            fn,
            runs=args.runs,
            strategy=args.strategy,
            seed=args.seed,
            max_duration=args.max_duration,
            report_dir=args.report_dir,
            database=False if args.no_db else None,
        )
    except BugFound as exc:
        print(exc.report.render(trace_limit=args.trace_limit))
        return 1
    print(stats.summary())
    return 0


def _cmd_replay(args: argparse.Namespace) -> int:
    from .explore import BugReport, _count_non_seed, _name_of, replay

    fn = _load(args.target)
    result = replay(fn, args.token, raise_on_failure=False)
    if result.failure is None:
        print(
            f"detangle: replaying {args.token}: no failure ({result.steps} steps, "
            f"virtual time {result.virtual_time:.6f}s)"
        )
        if result.trace is not None and args.trace:
            print(result.trace.render(limit=args.trace_limit))
        return 0
    report = BugReport(
        name=_name_of(fn),
        failure=result.failure,
        token=args.token,
        values=decode_token(args.token),
        runs=1,
        strategy="replay",
        seed=None,
        trace=result.trace,
        deviations=_count_non_seed(result),
        secondary=result.secondary,
        source="replay",
    )
    if args.html:
        from ._html import render_html

        Path(args.html).write_text(render_html(report), encoding="utf-8")
        report.html_report = args.html
    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    else:
        print(report.render(trace_limit=args.trace_limit))
    return 1


def _cmd_decode(args: argparse.Namespace) -> int:
    values = decode_token(args.token)
    nonzero = [(i, v) for i, v in enumerate(values) if v]
    print(f"{len(values)} decisions, {len(nonzero)} non-default:")
    for i, v in nonzero:
        print(f"  decision #{i}: {v}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="detangle", description="Deterministic simulation testing for Python asyncio."
    )
    parser.add_argument("--version", action="version", version=f"detangle {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("explore", help="explore the schedules of an async function")
    p.add_argument("target", help="module:function or path.py:function")
    p.add_argument("--runs", type=int, default=None)
    p.add_argument("--strategy", default=None)
    p.add_argument("--seed", type=lambda s: int(s, 0), default=None)
    p.add_argument("--max-duration", type=float, default=None)
    p.add_argument("--report-dir", default=None)
    p.add_argument("--no-db", action="store_true")
    p.add_argument("--trace-limit", type=int, default=80)
    p.set_defaults(func=_cmd_explore)

    p = sub.add_parser("replay", help="replay a token printed by a failing run")
    p.add_argument("token")
    p.add_argument("target", help="module:function or path.py:function")
    p.add_argument("--html", default=None, help="write an interactive HTML report")
    p.add_argument("--json", action="store_true", help="print the report as JSON")
    p.add_argument("--trace", action="store_true", help="print the trace even if the run passes")
    p.add_argument("--trace-limit", type=int, default=0, help="max trace lines (0 = all)")
    p.set_defaults(func=_cmd_replay)

    p = sub.add_parser("decode", help="show the decisions inside a token")
    p.add_argument("token")
    p.set_defaults(func=_cmd_decode)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
