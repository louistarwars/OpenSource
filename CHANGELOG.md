# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[semantic versioning](https://semver.org/).

## [0.1.0] - 2026-09-25

First public release.

### Added

- `SimLoop`: a deterministic asyncio event loop with virtual time, per-task lanes and
  recorded scheduling decisions. In default order it reproduces asyncio's own scheduling.
- `detangle.run`, `detangle.explore`, `detangle.replay` and the `@detangle.test` decorator.
- Strategies: `FIFO`, `RandomWalk`, `PCT`, bounded exhaustive `DFS`, `Replay`, `Portfolio`,
  and the default `auto` portfolio.
- Hypothesis-style shrinking of failing schedules, and replay tokens (`dt1-...`).
- Failure detection: exceptions, deadlocks (with lock holders and wait-for cycles), invariant
  violations, unobserved background exceptions, callback errors, step and time limits, task
  leaks.
- Fault injection: `maybe_timeout`, `inject_cancellation`, timer jitter.
- Nondeterministic inputs: `choice`, `randint`, `uniform`, `flip`, `shuffle`, plus per-run
  seeding of `random`.
- Simulated network: TCP-like streams (latency, coalescing, fragmentation, half-close, resets),
  UDP-like datagrams (loss, duplication, reordering), mailboxes, partitions, crashes and
  restarts.
- Linearizability checker (`detangle.lin`) with Register, KV, Counter, Set, FIFOQueue and
  Mutex models.
- pytest plugin, CLI (`detangle explore|replay|decode`), interactive HTML reports, JSON
  output, and a failing-example database.
