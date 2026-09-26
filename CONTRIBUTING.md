# Contributing to detangle

Thanks for helping make async Python more reliable! Bug reports, docs fixes, new examples and
features are all welcome.

## Development setup

```bash
git clone https://github.com/louistarwars/Detangle detangle && cd detangle
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
```

## Checks (the same ones CI runs)

```bash
pytest                      # the test suite, including the examples
ruff check src tests examples
ruff format --check src tests examples
mypy                        # strict mode
```

detangle supports CPython 3.10 to 3.14 and has no runtime dependencies. Please keep it that
way. The loop relies on a few asyncio internals (`Handle._run`, `Task._fut_waiter`,
`Task._log_traceback`...). Any new one needs a test that runs on every supported version.

## Guidelines

- **Determinism first.** Nothing in detangle may depend on real time, `id()`-based ordering
  or iteration over sets of objects. Use insertion-ordered dicts and lists.
- **Decisions default to asyncio.** Any new source of nondeterminism must go through
  `SimLoop.choose()` or `SimLoop.flip()`, with `0` / `False` meaning "what a stock event loop
  (or a perfect network) would do". That keeps replay, shrinking and DFS working for free.
- **Reports are the product.** When you add a failure mode, make sure the report explains it
  in plain words, with a location in *user* code.
- **Tests.** Every bug fix comes with a regression test. Every feature comes with tests, plus
  documentation in `docs/`.

## Reporting a bug in detangle

Please include the detangle version, the Python version, a minimal test, and the replay token
if a report was involved. If you believe detangle reports a schedule that cannot happen with
a real event loop, say which deviation (`^ ran ahead of ...`) looks impossible and why.

## Code of conduct

Be kind, assume good faith, and keep discussions technical. Harassment of any kind is not
tolerated.
