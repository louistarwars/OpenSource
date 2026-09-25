# The simulated network

Inside a simulation, asyncio's networking APIs are served by an in-memory network that is
deterministic, adversarial and fully under your control. Code written against asyncio streams or
protocols runs **unchanged**.

## Hosts

Every task runs on a simulated host. The default host is `"localhost"`.

```python
server_task = detangle.spawn(run_server(), host="db")    # this task and its children run on "db"
```

The host is stored in a context variable, so tasks created by a task inherit its host. Host
names are free-form (`"db"`, `"10.0.0.7"`, `"eu-west/primary"`). `"localhost"`, `"127.0.0.1"`,
`"0.0.0.0"` and `None` all mean *the current host*.

## TCP-like streams

```python
async def handle(reader, writer):
    writer.write(await reader.readline())
    await writer.drain()
    writer.close()

server = await asyncio.start_server(handle, "db", 5432)       # listens on host "db"
reader, writer = await asyncio.open_connection("db", 5432)    # from the current host
```

Supported: `loop.create_server`, `loop.create_connection`, `asyncio.start_server`,
`asyncio.open_connection`, their unix-socket variants (paths are addresses), `getaddrinfo`,
`server.sockets[0].getsockname()` (port `0` picks a free port), `serve_forever`, `close`,
`wait_closed`, `close_clients`/`abort_clients`, `write_eof` (half-close), `abort` (reset),
`pause_reading`/`resume_reading`, and `get_extra_info("peername" | "sockname" | "socket")`.
TLS parameters are accepted and ignored (the traffic is simulated in plaintext).

Semantics:

- **Reliable and ordered** per direction, like TCP.
- **Latency** for every segment, chosen from `NetConfig.latency` (explored; the minimum is the
  default). A connection costs one round trip (SYN, then SYN-ACK).
- **Coalescing.** Segments that are due at the same time are delivered in a single
  `data_received` call, the way a kernel buffer is read.
- **Fragmentation.** With `NetConfig.fragment = p`, each write is split, with probability `p`,
  into up to `max_fragments` pieces delivered separately. Code that assumes "one `read()` is
  one message" breaks here.
- **Refused connections** (`ConnectionRefusedError`) when nothing listens.
- **Partitions.** Segments are retransmitted until the link heals. If it stays down longer
  than `tcp_timeout`, both ends get `connection_lost(TimeoutError)`. Connecting across a
  partition raises `TimeoutError` after `connect_timeout`.

## UDP-like datagrams

```python
transport, protocol = await loop.create_datagram_endpoint(MyProtocol, local_addr=("dns", 53))
transport.sendto(b"query", ("dns", 53))
```

Each datagram gets its own latency, so datagrams get **reordered**. With `NetConfig.drop` and
`NetConfig.duplicate` they may also be **lost** or **delivered twice**. Partitions and crashed
hosts drop them silently.

## Mailboxes

For prototyping distributed protocols (Raft, Paxos, gossip, CRDTs), mailboxes carry Python
objects with datagram semantics:

```python
net = detangle.network()
inbox = net.mailbox(7000, host="n1")
inbox.send(("n2", 7000), {"type": "vote", "term": 3})
message, sender = await inbox.recv()
```

Messages are deep-copied when they are sent (`NetConfig.copy_messages`), as if serialised: the
sender and the receiver never share mutable state.

## Faults

```python
net = detangle.network()
net.partition(["n1"], ["n2", "n3"])   # groups can't talk; hosts not listed talk to everyone
net.disconnect("n1", "n2")            # cut one link (both directions)
net.reconnect("n1", "n2")
net.heal()                            # remove every cut
net.crash("n1")                       # cancel its tasks, close its sockets silently
net.restart("n1")                     # accept connections again (re-spawn your server)
net.is_up("n1"); net.reachable("n1", "n2"); net.hosts
```

A **crash** cancels every task running on the host, closes its listeners, mailboxes and
datagram endpoints, and makes its side of each connection vanish without a FIN. Peers find out
the way TCP would: their connection is reset after `tcp_timeout`. New connections to the host
fail until `restart`. Note that the cancelled tasks do run their `finally` blocks. Any network
operation they attempt there fails, because the host is down.

Faults are commands your test issues at a point you choose, often at a random one:

```python
await asyncio.sleep(detangle.uniform(0, 0.5))
net.partition(["n1"], ["n2", "n3"])
```

## NetConfig

| Field | Default | Meaning |
| --- | --- | --- |
| `latency` | `(0.0005, 0.005)` | Range of one-way latencies (seconds). |
| `latency_steps` | `4` | Number of distinct latency values explored. |
| `drop` | `0.0` | Probability that a datagram or mailbox message is lost. |
| `duplicate` | `0.0` | Probability that it is delivered twice. |
| `fragment` | `0.0` | Probability that a TCP write is split. |
| `max_fragments` | `3` | Maximum number of pieces per split write. |
| `connect_timeout` | `10.0` | Seconds before a connection attempt across a partition fails. |
| `tcp_timeout` | `30.0` | Seconds a connection survives a partition or a crashed peer. |
| `retransmit_interval` | `0.2` | Retry period while a link is down. |
| `copy_messages` | `True` | Deep-copy mailbox messages. |

```python
@detangle.test(net={"latency": (0.001, 0.1), "drop": 0.05, "fragment": 0.2})
async def test_cluster(): ...
```

All of these are **decisions**, so a failure that needs a lost message or a slow link is shrunk
down to the few faults that matter, and its token replays them exactly.

## Real I/O

Raw sockets (`loop.sock_*`), subprocesses, pipes and signal handlers raise
`detangle.RealIOError`, a subclass of `NotImplementedError`. Anything built on asyncio
transports and protocols works over the simulated network. Libraries that manage their own
sockets must be faked at a higher level.
