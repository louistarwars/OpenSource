# detangle guide

This guide walks through every public API. For the internals (lanes, decisions, soundness) see
[how-it-works.md](how-it-works.md).

- [Installing](#installing)
- [Three ways to run code](#three-ways-to-run-code)
- [Writing tests](#writing-tests)
- [Configuration](#configuration)
- [Strategies](#strategies)
- [Reading a failure report](#reading-a-failure-report)
- [Replaying and debugging](#replaying-and-debugging)
- [Faults, invariants and nondeterministic inputs](#faults-invariants-and-nondeterministic-inputs)
- [What counts as a failure](#what-counts-as-a-failure)
- [pytest plugin reference](#pytest-plugin-reference)
- [Command line](#command-line)
- [Environment variables](#environment-variables)
- [Tips](#tips)

## Installing

```bash
pip install detangle
```

detangle is pure Python, has no dependencies and supports CPython 3.10 to 3.14. The pytest
plugin registers itself automatically.

## Three ways to run code

| Function | What it does | Returns |
| --- | --- | --- |
| `detangle.run(main, ...)` | One run, like `asyncio.run`, in virtual time. With no `seed`/`strategy`, the order is exactly asyncio's. | `main`'s return value, or raises its exception |
| `detangle.explore(fn, ...)` | Many runs under a strategy. On failure: shrink, replay with a trace, raise. | `ExploreStats`, or raises `detangle.BugFound` |
| `@detangle.test(...)` | Decorator. Turns an `async def` test into a sync function that calls `explore`. | — |

```python
import asyncio
import detangle

async def main():
    await asyncio.sleep(3600)
    return asyncio.get_running_loop().time()

detangle.run(main)                    # 3600.0, instantly
detangle.run(main, seed=42)           # one reproducible random schedule
detangle.run(main, replay="dt1-...")  # re-run a recorded schedule
detangle.run(main, trace=True)        # print the trace if it fails

stats = detangle.explore(main, runs=500)
print(stats.summary())
```

`explore` needs a **function** (it calls it once per run). `run` also accepts a coroutine
object.

Every run starts from scratch: build the objects under test *inside* the function, so that no
state leaks from one run to the next. Module-level state (global caches, class attributes,
`itertools.count()` at module level) breaks replay.

## Writing tests

```python
@detangle.test
async def test_default(): ...

@detangle.test(runs=1000, strategy="pct:3", timer_jitter=0.005, net={"fragment": 0.2})
async def test_configured(): ...
```

The decorated function is a normal synchronous function, so pytest, unittest or a plain script
can call it. Arguments are forwarded to every run, so pytest fixtures work. Note that fixtures
are created **once per test**, not once per run: don't put mutable state under test in them.

With the pytest plugin, the marker is equivalent to the decorator:

```python
@pytest.mark.detangle(runs=300)
async def test_marked(tmp_path): ...
```

`@detangle.test` arguments:

| Argument | Default | Meaning |
| --- | --- | --- |
| `runs` | `200` | Maximum number of runs. |
| `strategy` | `"auto"` | Name (`auto`, `pct[:depth]`, `random`, `dfs[:max_delays]`, `fifo`) or a `Strategy` object. |
| `seed` | random | Seed of the exploration. It is printed in failure reports. |
| `max_duration` | `None` | Stop exploring after this many seconds of real time. |
| `shrink` | `True` | Shrink failing schedules. |
| `database` | `True` | Save failing tokens in `.detangle/` and replay them first next time. A path string selects another directory. |
| `report_dir` | `None` | Write an HTML report per failure there. |
| `config` | `None` | A `SimConfig`; keyword options below override its fields. |
| `**options` | | Any `SimConfig` field (`timer_jitter=...`, `net={...}`, ...). |

## Configuration

`SimConfig` fields, which can be passed as keyword options to `run`, `explore` and `test`:

| Field | Default | Meaning |
| --- | --- | --- |
| `reorder` | `True` | Explore the order of simultaneously ready tasks. `False` keeps strict asyncio FIFO. |
| `timer_jitter` | `0.0` | Timers may fire up to this many seconds late (explored). |
| `jitter_steps` | `4` | Number of distinct jitter values explored. |
| `step_cost` | `0.0` | Virtual seconds consumed by each step (models CPU time). |
| `max_steps` | `200_000` | A run that takes more steps fails (livelock, busy wait). |
| `max_time` | `None` | A run whose virtual clock goes past this fails (runaway retries). |
| `fail_on_unobserved` | `True` | An exception in a task nobody awaited is a failure. |
| `fail_on_callback_error` | `True` | An exception escaping a callback (protocol, `call_soon`...) is a failure. |
| `check_leaks` | `False` | Tasks still pending when the test returns are a failure. |
| `seed_random` | `True` | Seed the global `random` module per run (restored afterwards). |
| `patch_time` | `False` | Make `time.time/monotonic/perf_counter` return virtual time during steps. |
| `net` | `NetConfig()` | Network parameters (a `NetConfig` or a dict of its fields). |

`NetConfig` fields are described in [network.md](network.md).

## Strategies

| Strategy | Use it for |
| --- | --- |
| `"auto"` (default) | A portfolio: one run in asyncio's own order, then PCT (depth 2 and 3), random walks and random priorities, in rotation. |
| `PCT(seed, depth=3)` | Probabilistic Concurrency Testing. Each task gets a random priority and `depth - 1` priority drops happen at random steps. It finds a bug needing `d` ordering constraints with probability at least `1/(n·k^(d-1))` per run. |
| `RandomWalk(seed)` | Every decision is uniformly random. Cheap and noisy; shrinking cleans up. |
| `DFS(max_delays=2)` | Exhaustive: every schedule whose deviations sum to at most `max_delays`. When `stats.exhausted` is true, no failing schedule exists within the bound. |
| `FIFO()` | A single run in asyncio's order. |
| `Replay(values)` | Replays a decision list. |
| `Portfolio([...])` | Round-robin over strategies, one per run. |

To write your own, subclass `detangle.Strategy` and implement `schedule(lanes)`, `choose(n, kind)`
and `flip(p, kind)`. For every question, `0` or `False` means "asyncio's default".

## Reading a failure report

```text
detangle found a bug in tests.test_pool.test_timeouts after 7 runs [pct(depth=3, seed=1)]

  AssertionError: 1 connection(s) leaked
    at tests/test_pool.py:59 in scenario: assert pool.in_use == 0, ...

Minimal failing schedule: 1 deviation from asyncio's default behaviour (shrunk from 16 in 21 replays).

Interleaving (* = the scheduler deviated from asyncio's FIFO order here):
    14    0  query#6   await   sleep  tests/test_pool.py:35  | await asyncio.sleep(0.01)
    15    0  detangle  fault   injected cancellation into query#6 at tests/test_pool.py:35
    ...
Reproduce this exact run:
    DETANGLE_REPLAY=dt1-... pytest "tests/test_pool.py::test_timeouts"
```

- **Deviations** count the decisions that differ from asyncio's default: a task run ahead of
  an older one, a late timer, a non-minimal latency, an injected fault. "0 deviations" means
  the test fails on a stock event loop too.
- In the **trace**, each row is a step. `await` rows show where the task suspended (the
  innermost frame of *your* code, plus what it awaits, such as `Lock.acquire` or
  `StreamReader.readline`). A `*` marks a scheduling deviation and names the tasks it
  overtook. `net` rows show packets, partitions and crashes. `fault` rows show injected
  cancellations. `note` rows come from `detangle.note()`.
- **Deadlock** reports list each blocked task, what it waits for, who holds it, and the
  wait-for cycle.

## Replaying and debugging

```python
detangle.replay(test_fn, "dt1-...")                       # raises BugFound with a full trace
result = detangle.replay(test_fn, "dt1-...", raise_on_failure=False)
print(result.trace.render(limit=0))                        # the whole trace
```

A replay is exact, so you can put breakpoints or `print` calls in your code and replay as often
as you like. `detangle.note("msg")` adds a line to the trace. The CLI can also write an HTML
report or JSON:

```bash
detangle replay dt1-... tests/test_x.py:test_fn --html report.html
detangle replay dt1-... tests/test_x.py:test_fn --json
detangle decode dt1-...          # list the non-default decisions inside a token
```

A replay token only makes sense for the same test code and the same configuration. If you
change the code, the token may replay a different (but still valid) schedule.

## Faults, invariants and nondeterministic inputs

All of these must be called from code running inside a simulation.

```python
detangle.invariant(lambda: total() == 100, "conservation")   # checked after every step
detangle.note("leader elected")                              # shows up in traces

await detangle.maybe_timeout(op())         # returns op's result, or raises TimeoutError after
                                           # cancelling op at one of its first 8 awaits
detangle.inject_cancellation(task, points=8)  # same, for an existing task (CancelledError)

detangle.choice(["a", "b"])     # explored; shrinks towards the first option
detangle.randint(0, 10)         # shrinks towards 0
detangle.uniform(0.0, 1.0)      # one of 64 values; shrinks towards 0.0
detangle.flip(0.1)              # True with probability 0.1; shrinks towards False
detangle.shuffle(items)         # default = identity

detangle.now()                  # virtual time
detangle.in_simulation()        # True inside a simulation
detangle.spawn(coro, host="db") # create a task on a simulated host
detangle.network()              # the SimNetwork of this run
```

The global `random` module is seeded from the schedule, so `random.random()` in your code is
reproducible as well.

## What counts as a failure

| Kind | Meaning |
| --- | --- |
| `exception` | The test function raised. |
| `deadlock` | Nothing can run, no timer is pending, and the test has not returned. |
| `invariant` | A `detangle.invariant` check failed after some step. |
| `unobserved-exception` | A background task died with an exception nobody retrieved. |
| `callback-error` | An exception escaped a callback (a protocol method, `call_soon`...). |
| `step-limit` / `time-limit` | The run exceeded `max_steps` / `max_time`. |
| `task-leak` | With `check_leaks=True`: tasks still pending when the test returned. |

When the test returns, remaining tasks are cancelled and drained, exactly as `asyncio.run`
does.

## pytest plugin reference

| Option | Effect |
| --- | --- |
| `--detangle-runs=N` | Override the number of runs of every detangle test. |
| `--detangle-strategy=S` | Override the strategy. |
| `--detangle-seed=N` | Fix the exploration seed. |
| `--detangle-replay=TOKEN` | Replay this token instead of exploring. |
| `--detangle-max-duration=SECONDS` | Time budget per test. |
| `--detangle-report-dir=DIR` | Write interactive HTML reports. |
| `--detangle-no-db` | Don't read or write `.detangle/`. |
| `--detangle-no-shrink` | Report failures unshrunk (faster). |

Command-line options and environment variables **override** the values written in the code:
they express what you want for this particular invocation.

## Command line

```bash
detangle explore path/to/file.py:function [--runs N] [--strategy S] [--seed N]
                 [--max-duration S] [--report-dir DIR] [--no-db]
detangle replay TOKEN path/to/file.py:function [--html FILE] [--json] [--trace]
detangle decode TOKEN
```

Targets can be `path.py:function` or `package.module:function`. A function decorated with
`@detangle.test` is unwrapped automatically.

## Environment variables

`DETANGLE_RUNS`, `DETANGLE_STRATEGY`, `DETANGLE_SEED`, `DETANGLE_REPLAY`,
`DETANGLE_MAX_DURATION`, `DETANGLE_REPORT_DIR`, `DETANGLE_SHRINK` (`0` disables shrinking),
`DETANGLE_DATABASE` (a path, or `off`), `DETANGLE_VERBOSE` (print a summary of every
exploration).

## Tips

- **Start with the default order.** `detangle.run(test)` must pass before exploring. If it
  fails, the report says "0 deviations": that is a plain bug.
- **Model latency with sleeps.** A fake dependency that returns instantly hides races. Give it
  an `await asyncio.sleep(...)`, or `detangle.uniform(...)` for variable latency.
- **Run more in CI nightly** with `--detangle-runs=5000 --detangle-max-duration=60`, and keep
  the `.detangle/` database out of version control (it ignores itself).
- **Prove fixes.** After fixing a race, `strategy=detangle.DFS(max_delays=3)` with
  `stats.exhausted` gives a bounded guarantee instead of "it didn't fail this time".
- **Keep runs small.** Shorter tests explore faster and shrink better. Three tasks and a few
  operations each find most bugs; large runs are for nightly jobs.
