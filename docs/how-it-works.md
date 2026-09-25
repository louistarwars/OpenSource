# How detangle works

## The problem

A stock asyncio event loop is deterministic *given the timing of the outside world*: it runs
ready callbacks in FIFO order and wakes timers and I/O when they are due. In a unit test the
outside world is fake, so the timing never varies: the test runs **one** interleaving, the same
one every time. In production, network and disk latencies vary on every request, and so does
the interleaving. Bugs live in the interleavings your tests never run.

detangle makes those variations explicit, controllable and reproducible.

## SimLoop: a loop that asks before it acts

`detangle.SimLoop` implements `asyncio.AbstractEventLoop` from scratch. It does not subclass
`BaseEventLoop`: there are no selectors and no real clock.

### Lanes

asyncio executes *handles*. `Task.__step` and `Task.__wakeup` are handles, and so are future
callbacks, timers and `call_soon` callbacks. `SimLoop` sorts every ready handle into a **lane**:

- one lane per task (identified through the handle's bound `__self__`), holding that task's
  steps in order;
- one shared **callback lane** for everything else (future callbacks, fired timers, protocol
  callbacks), kept in FIFO order;
- one lane per simulated executor job (`run_in_executor`, `asyncio.to_thread`).

Lanes are numbered in creation order, so their identity is stable from run to run.

### Decisions

When more than one lane is ready, the loop asks the strategy: *which lane runs next?* The lanes
are sorted by the age of their oldest handle, so answer `0` is exactly what asyncio's FIFO would
do. Other sources of nondeterminism are also turned into questions whose answer `0` is the
default:

| Decision | Question | `0` means |
| --- | --- | --- |
| `sched` | Which ready lane runs next? | the oldest (asyncio's order) |
| `timer` | How late does this timer fire? (`timer_jitter`) | on time |
| `net` | Which latency for this packet? Split this write? | minimum latency, no split |
| `fault` | Drop or duplicate this datagram? Cancel this task at its k-th await? | no fault |
| `data` | `detangle.choice/randint/uniform/flip/shuffle` | the first or smallest option |
| `seed` | Seed for the global `random` module | seed 0 |

Every decision is appended to the run's **decision list**. A decision with a single possible
answer is not recorded.

### Virtual time

When no lane is ready, the clock jumps to the next timer. Steps take zero virtual time unless
`step_cost` says otherwise. Because time only moves when everything is blocked, a virtual clock
is exactly as deterministic as the decisions.

## Replaying and tokens

A run is a pure function of its decision list (given the same code). Replaying feeds the same
integers back. Values past the end of the list default to `0`, and out-of-range values are
clamped, so **any** list of integers is a valid schedule. A token is the list, with trailing
zeros stripped, varint-encoded, zlib-compressed when that helps, and base64url-encoded with a
`dt1-` prefix. Most failing schedules are a few dozen bytes.

## Strategies

- **FIFO**: all zeros. This is asyncio.
- **RandomWalk**: uniform answers. It finds shallow bugs quickly, but its schedules are noisy.
- **PCT** (Burckhardt, Kothari, Musuvathi, Nagarakatte, ASPLOS 2010): random lane priorities
  plus `d - 1` random priority-drop points. For a bug of depth `d` (one needing `d` ordering
  constraints), each run finds it with probability at least `1/(n·k^(d-1))`, where `n` is the
  number of lanes and `k` the number of steps. detangle adapts `k` to the observed run length.
- **DFS**: stateless model checking by replay. After each run, backtrack to the deepest
  decision that can be incremented while keeping the **sum of all answers** at most
  `max_delays`. This is delay-bounded scheduling (Emmi, Qadeer, Rakamarić, POPL 2011),
  generalised to every decision kind. Decisions with more than `max_branch` options (seeds,
  `uniform`) stay at 0. When the search ends, every schedule within the bound was checked.
- **auto**: one FIFO run first (the most natural schedule), then a rotation of PCT (depth 2
  and 3), random walk and random priorities (PCT depth 1).

## Shrinking

A failure has a **signature**: its kind, the exception type, and the file and function where
it was raised (for deadlocks, the set of blocked coroutines). The shrinker looks for a
"simpler" decision list with the same signature. Simpler means, in this order: fewer non-zero
decisions, then a smaller sum, then a shorter list. It tries, in passes:

1. dropping suffixes (asyncio's default from some point on);
2. zeroing chunks of decisions, from half the list down to single decisions;
3. lowering individual values;
4. deleting decisions, which realigns the rest of the schedule.

Every candidate is replayed, and a candidate is kept when it fails the same way. The result
usually has one or two deviations: the essence of the race. The final schedule is replayed once
more with tracing on to produce the report.

## Deadlock analysis

When no lane is ready, no timer is pending and the main task is not done, the loop analyses
every pending task:

- it walks the suspended coroutine chain (`cr_await`) to the innermost frame and recognises
  asyncio primitives by their code objects (`Lock.acquire`, `Event.wait`, `Queue.get`,
  `Condition.wait`, `StreamReader` waits, `gather`, `TaskGroup`, `wait_for`...);
- it names the primitive by searching the user frames for a variable that refers to it,
  preferring names that appear on the executing line;
- it finds **who holds** locks and semaphores. `asyncio.Lock` and `asyncio.Semaphore` are
  wrapped once, idempotently, so that inside a simulation acquisitions and releases are
  recorded. Outside a simulation the wrappers are pure pass-throughs;
- it builds the wait-for graph and reports its cycles.

## Teardown

When the main coroutine finishes, or fails, the loop behaves like `asyncio.run`. It cancels
the remaining tasks and drains them, runs pending async-generator finalizers, and then checks
for background tasks that died with an exception nobody retrieved. Teardown never records
decisions and never consults the strategy, so it cannot change a replay.

## Soundness: what a reported schedule means

detangle explores **two** kinds of nondeterminism:

1. **Environment nondeterminism**: timer lateness, network latency, loss, duplication,
   fragmentation, partitions, crashes, injected timeouts. These are faithful: each explored
   schedule is one that a real asyncio loop can produce under some timing of the outside world.
2. **Reordering of ready tasks** (`reorder=True`). This models "the `await` this task was
   suspended on took a little longer than the other one". For awaits on real I/O, that is
   exactly what happens in production. For purely in-memory wake-ups, where task A sets an
   event that wakes task B, asyncio guarantees FIFO order, and some explored orders are then
   stricter than reality.

In practice (2) is what finds most bugs, and code that is only correct thanks to asyncio's FIFO
tie-breaking is fragile anyway. When a report depends on such a deviation, the trace shows it
(`^ ran ahead of ...`), and `reorder=False` restricts exploration to (1).

## Determinism checklist

A replay is exact if everything nondeterministic in the code under test goes through the loop:

- time comes from `loop.time()`, `asyncio.sleep` or timeouts (or `patch_time=True`);
- randomness comes from `random` (seeded per run) or `detangle.*` helpers;
- I/O goes through asyncio streams, protocols or datagram endpoints (simulated);
- no real threads, no iteration over `id()`-hashed sets whose order matters, and no state
  shared between runs.

If a final replay does not reproduce the failure, the report says so explicitly.
