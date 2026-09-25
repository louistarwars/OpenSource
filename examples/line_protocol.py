"""Testing a real asyncio-streams server over an adversarial network.

The server and client below use ``asyncio.start_server`` and
``asyncio.open_connection`` -- unmodified.  Inside a simulation they talk over
detangle's in-memory network, where writes can be split into several reads
(TCP does that!).  The naive server assumes one ``read()`` == one command.

    python examples/line_protocol.py
"""

from __future__ import annotations

import asyncio

import detangle


class KVServer:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}

    async def handle_naive(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        while chunk := await reader.read(1024):
            for line in chunk.decode().splitlines():  # BUG: a command may span two reads
                writer.write(self.execute(line).encode() + b"\n")
            await writer.drain()
        writer.close()

    async def handle_framed(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        while line := await reader.readline():  # buffers until a full line arrived
            writer.write(self.execute(line.decode().strip()).encode() + b"\n")
            await writer.drain()
        writer.close()

    def execute(self, command: str) -> str:
        parts = command.split()
        if parts[:1] == ["SET"] and len(parts) == 3:
            self.data[parts[1]] = parts[2]
            return "OK"
        if parts[:1] == ["GET"] and len(parts) == 2:
            return self.data.get(parts[1], "(nil)")
        return f"ERR unknown command {command!r}"


async def scenario(handler_name: str) -> None:
    server = KVServer()
    await asyncio.start_server(getattr(server, handler_name), "kv", 6379)
    reader, writer = await asyncio.open_connection("kv", 6379)
    writer.write(b"SET greeting hello\nGET greeting\n")
    await writer.drain()
    assert await reader.readline() == b"OK\n"
    assert await reader.readline() == b"hello\n"
    writer.close()


async def buggy() -> None:
    await scenario("handle_naive")


async def fixed() -> None:
    await scenario("handle_framed")


NET = {"fragment": 0.3, "latency": (0.001, 0.02)}


def demo() -> tuple[str, str]:
    detangle.run(buggy)  # fine on a perfect network
    try:
        detangle.explore(buggy, runs=300, seed=1, net=NET, database=False)
        found = "no bug found (unexpected)"
    except detangle.BugFound as bug:
        found = bug.report.render(trace_limit=20)
    stats = detangle.explore(fixed, runs=300, seed=1, net=NET, database=False)
    return found, stats.summary()


if __name__ == "__main__":
    found, check = demo()
    print(found)
    print()
    print(check)
