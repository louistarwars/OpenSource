"""Jepsen in a unit test: a replicated key-value store with stale reads.

Three nodes talk over detangle's simulated network (``Mailbox``: messages can
be delayed, reordered, or lost).  The primary acknowledges writes *before* its
replicas have applied them, and clients read from any node.  Every client
operation is recorded in a ``detangle.History`` and checked for
linearizability against a key-value model.

    python examples/replicated_kv.py
"""

from __future__ import annotations

import asyncio
import itertools
from typing import Any

import detangle
from detangle.lin import KV, History

PORT = 7000
NODES = ["n1", "n2", "n3"]
PRIMARY = "n1"


async def node(name: str) -> None:
    net = detangle.network()
    box = net.mailbox(PORT, host=name)
    data: dict[str, Any] = {}
    while True:
        msg, src = await box.recv()
        if msg["type"] == "put":
            data[msg["key"]] = msg["value"]
            for peer in NODES:
                if peer != name:  # asynchronous replication...
                    box.send(
                        (peer, PORT),
                        {"type": "replicate", "key": msg["key"], "value": msg["value"]},
                    )
            box.send(src, {"type": "ok", "id": msg["id"]})  # ...acknowledged immediately
        elif msg["type"] == "replicate":
            data[msg["key"]] = msg["value"]
        elif msg["type"] == "get":
            box.send(src, {"type": "value", "id": msg["id"], "value": data.get(msg["key"])})


class Client:
    def __init__(self, name: str) -> None:
        self.name = name
        self.ids = itertools.count()
        self.box = detangle.network().mailbox(PORT, host=name)
        self.pending: dict[int, asyncio.Future[Any]] = {}
        self.receiver = asyncio.ensure_future(self._receive())

    async def _receive(self) -> None:
        while True:
            msg, _ = await self.box.recv()
            future = self.pending.pop(msg["id"], None)
            if future is not None and not future.done():
                future.set_result(msg.get("value"))

    async def call(self, target: str, message: dict[str, Any]) -> Any:
        request_id = next(self.ids)
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        self.box.send((target, PORT), {**message, "id": request_id})
        return await asyncio.wait_for(future, timeout=1.0)


async def scenario(read_from_any_node: bool) -> None:
    servers = [detangle.spawn(node(name), host=name) for name in NODES]
    history = History()

    async def client(name: str) -> None:
        c = Client(name)
        for i in range(3):
            if detangle.flip():
                value = f"{name}-{i}"
                op = c.call(PRIMARY, {"type": "put", "key": "x", "value": value})
                await history.call(name, "put", ("x", value), op)
            else:
                target = detangle.choice(NODES) if read_from_any_node else PRIMARY
                op = c.call(target, {"type": "get", "key": "x"})
                await history.call(name, "get", "x", op)
        c.receiver.cancel()

    await asyncio.gather(*(detangle.spawn(client(f"c{i}"), host=f"c{i}") for i in range(3)))
    for server in servers:
        server.cancel()
    history.assert_linearizable(KV())


async def buggy() -> None:
    await scenario(read_from_any_node=True)


async def fixed() -> None:
    await scenario(read_from_any_node=False)


NET = {"latency": (0.001, 0.05)}


def demo() -> tuple[str, str]:
    try:
        detangle.explore(buggy, runs=500, seed=1, net=NET, database=False)
        found = "no bug found (unexpected)"
    except detangle.BugFound as bug:
        found = bug.report.render(trace_limit=25)
    stats = detangle.explore(fixed, runs=300, seed=1, net=NET, database=False)
    return found, stats.summary()


if __name__ == "__main__":
    found, check = demo()
    print(found)
    print()
    print(check)
