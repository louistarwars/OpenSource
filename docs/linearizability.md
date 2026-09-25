# Linearizability checking

*Linearizability* is the gold standard for concurrent objects and distributed data stores:
every operation appears to take effect at a single instant between its invocation and its
response, and the resulting sequential history is legal for the object's specification.
Violations are stale reads, lost writes, duplicated dequeues, split-brain leaders...

detangle records concurrent histories inside a simulation and checks them with the Wing & Gong
algorithm, with Lowe's memoisation and P-compositionality (Horn & Kroening), the approach
behind Knossos and Porcupine. It combines naturally with exploration: `assert_linearizable()`
raises `NotLinearizable`, an `AssertionError`, so detangle searches for the schedule that breaks
linearizability and shrinks it.

## Recording a history

```python
from detangle.lin import History, KV

history = History()

async def client(name):
    await history.call(name, "put", ("x", 1), store.put("x", 1))
    value = await history.call(name, "get", "x", store.get("x"))

await asyncio.gather(*(client(f"c{i}") for i in range(3)))
history.assert_linearizable(KV())
```

`history.call(process, f, arg, awaitable)` records the invocation, awaits, and records the
response. If the awaitable raises, the outcome is **unknown** (a timeout may or may not have
applied the write), and the checker lets the operation take effect at any point after its
invocation, or never. Exceptions listed in `fail_on=(...)` mean "definitely did not happen".

You can also record manually:

```python
op = history.invoke("c1", "put", ("x", 1))
...
history.ok(op, output)      # or history.fail(op) / history.info(op)
```

## Built-in models

| Model | Operations (`f(arg) -> output`) |
| --- | --- |
| `Register(initial)` | `read() -> v`, `write(v)`, `cas((expected, new)) -> bool` (`get`/`put`/`set` are aliases) |
| `KV(initial={})` | `get(key) -> v or None`, `put((key, v))`, `delete(key)`, `cas((key, expected, new)) -> bool`, checked independently per key |
| `Counter(initial=0)` | `add(n) -> new value or None`, `read() -> v` |
| `Set()` | `add(x) -> bool or None`, `remove(x) -> bool or None`, `contains(x) -> bool`, `read() -> collection` |
| `FIFOQueue()` | `enqueue(x)`, `dequeue() -> x`, where `None` means the queue was empty |
| `Mutex()` | `acquire()`, `release()`, checked per process |

## Writing a model

```python
from detangle.lin import Model

class BankAccount(Model):
    name = "account"

    def init(self):
        return 0                                  # hashable state

    def step(self, state, op):
        if op.f == "deposit":
            return True, state + op.arg
        if op.f == "withdraw":
            if op.status == "ok" and op.output is False:
                return True, state                # refused: no change
            return (state >= op.arg), state - op.arg
        if op.f == "balance":
            return (op.status != "ok" or op.output == state), state
        raise ValueError(op.f)
```

`step` returns `(legal, new_state)`. States must be hashable (use tuples or frozensets). For an
operation whose outcome is unknown (`op.status != "ok"`), the output is meaningless: accept any.
Override `partition(ops)` to split the history into independent sub-histories, as `KV` does per
key. This is P-compositionality, and it makes large histories cheap to check.

## Reading a counterexample

```text
history is not linearizable with respect to the kv model
longest linearizable prefix (4 of 9 operations):
    1. 'c2': get('x') -> None
    2. 'c2': put('x', 'c2-1') -> None
    3. 'c0': get('x') -> 'c2-1'
    4. 'c1': get('x') -> 'c2-1'
  state afterwards: 'c2-1'
no valid ordering can explain: 'c2': get('x') -> None (invoked at event 7, returned at event 8)
timeline (real-time order, left to right; > = never returned):
  'c0' [-----------1-----------]  [---6----]  [---8----]
  'c1'    [------------2-------------]  [---7----]  [--9--]
  'c2'       [3-]  [4-]  [5-]
```

Read it as: the checker could order the first four operations consistently. Nothing explains
operation 5, where `c2` reads `None` right after its own write of `'c2-1'` was acknowledged:
a stale read from a replica. The timeline shows real-time precedence: a bar that ends before
another begins *must* be ordered first.

## Complexity

Checking linearizability is NP-complete in general. The memoised search is fast for the
histories a unit test produces (tens to a few hundred operations, a handful of processes),
especially with a partitioned model. `check(..., budget=N)` bounds the search and raises
`RuntimeError` beyond it.
