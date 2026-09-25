"""The simulated network: streams, protocols, datagrams, mailboxes, faults."""

from __future__ import annotations

import asyncio

import pytest

import detangle
from detangle import BugFound


async def echo_roundtrip() -> tuple[bytes, bytes]:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while line := await reader.readline():
            writer.write(line.upper())
            await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, "srv", 7000)
    reader, writer = await asyncio.open_connection("srv", 7000)
    writer.write(b"hello\nworld\n")
    await writer.drain()
    a = await reader.readline()
    b = await reader.readline()
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    return a, b


def test_streams_work_unmodified() -> None:
    assert detangle.run(echo_roundtrip) == (b"HELLO\n", b"WORLD\n")


def test_streams_under_adversarial_network() -> None:
    stats = detangle.explore(
        echo_roundtrip, runs=150, seed=3, net={"fragment": 0.5, "latency": (0.001, 0.05)}
    )
    assert stats.runs == 150


def test_latency_is_virtual_and_explored() -> None:
    async def rtt() -> float:
        loop = asyncio.get_running_loop()
        await asyncio.start_server(lambda r, w: w.close(), "srv", 1)
        start = loop.time()
        _, writer = await asyncio.open_connection("srv", 1)
        writer.close()
        return loop.time() - start

    default = detangle.run(rtt, net={"latency": (0.01, 0.1)})
    assert default == pytest.approx(0.02)  # SYN + SYN-ACK at minimum latency
    samples = {round(detangle.run(rtt, seed=s, net={"latency": (0.01, 0.1)}), 6) for s in range(20)}
    assert len(samples) > 1
    assert all(0.02 <= s <= 0.2 + 1e-9 for s in samples)


def test_ephemeral_port_and_sockets() -> None:
    async def main() -> tuple[str, int]:
        server = await asyncio.start_server(lambda r, w: None, "0.0.0.0", 0)
        host, port = server.sockets[0].getsockname()
        _, writer = await asyncio.open_connection("localhost", port)
        peer = writer.get_extra_info("peername")
        assert peer == (host, port)
        writer.close()
        return host, port

    host, port = detangle.run(main)
    assert host == "localhost" and port > 0


def test_connection_refused() -> None:
    async def main() -> None:
        await asyncio.open_connection("nobody", 1234)

    with pytest.raises(ConnectionRefusedError):
        detangle.run(main)


def test_framing_bug_found_by_fragmentation() -> None:
    async def main() -> None:
        received: list[bytes] = []

        class Collector(asyncio.Protocol):
            def data_received(self, data: bytes) -> None:
                received.append(data)  # BUG: assumes one read == one message

        loop = asyncio.get_running_loop()
        await loop.create_server(Collector, "srv", 9)
        _, writer = await asyncio.open_connection("srv", 9)
        writer.write(b"PING")
        await asyncio.sleep(0.1)
        writer.write(b"PONG")
        await asyncio.sleep(0.1)
        assert received == [b"PING", b"PONG"], received

    detangle.run(main)  # fine on a perfect network
    with pytest.raises(BugFound) as info:
        detangle.explore(main, runs=200, seed=1, net={"fragment": 0.3})
    assert "fragmented" in str(info.value)


def test_writes_are_coalesced_like_tcp() -> None:
    async def main() -> list[bytes]:
        chunks: list[bytes] = []

        class Collector(asyncio.Protocol):
            def data_received(self, data: bytes) -> None:
                chunks.append(data)

        await asyncio.get_running_loop().create_server(Collector, "srv", 9)
        _, writer = await asyncio.open_connection("srv", 9)
        writer.write(b"a")
        writer.write(b"b")
        writer.write(b"c")
        await asyncio.sleep(1)
        return chunks

    assert detangle.run(main) == [b"abc"]


def test_eof_and_half_close() -> None:
    async def main() -> bytes:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            data = await reader.read()  # until EOF
            writer.write(data[::-1])
            await writer.drain()
            writer.close()

        await asyncio.start_server(handle, "srv", 5)
        reader, writer = await asyncio.open_connection("srv", 5)
        writer.write(b"abc")
        writer.write_eof()
        reply = await reader.read()
        writer.close()
        return reply

    assert detangle.run(main) == b"cba"


def test_partition_blocks_then_heals() -> None:
    async def main() -> tuple[str, bytes]:
        net = detangle.network()

        async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            writer.write(await reader.read(100))
            await writer.drain()
            writer.close()

        await asyncio.start_server(serve, "db", 5432)
        net.partition(["localhost"], ["db"])
        outcome = "connected"
        try:
            await asyncio.open_connection("db", 5432)
        except TimeoutError:
            outcome = f"timeout after {detangle.now():.0f}s"
        net.heal()
        reader, writer = await asyncio.open_connection("db", 5432)
        writer.write(b"ping")
        return outcome, await reader.read(10)

    assert detangle.run(main) == ("timeout after 10s", b"ping")


def test_partition_during_connection_delays_then_resets() -> None:
    async def main() -> str:
        net = detangle.network()
        writers = []  # keep server-side writers alive (3.13+ closes GC'd writers)
        await asyncio.start_server(lambda r, w: writers.append(w), "db", 1)
        reader, writer = await asyncio.open_connection("db", 1)
        await asyncio.sleep(0.1)
        net.partition(["localhost"], ["db"])
        writer.write(b"lost?")
        try:
            await reader.read(1)
        except (ConnectionError, TimeoutError) as exc:
            return f"{type(exc).__name__} at {detangle.now():.0f}s"
        return "no error"

    assert detangle.run(main, net={"tcp_timeout": 30}) == "TimeoutError at 30s"


def test_crash_cancels_host_tasks_and_resets_peers() -> None:
    async def main() -> list[str]:
        log: list[str] = []
        net = detangle.network()

        async def server_main() -> None:
            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                try:
                    await reader.read()
                except asyncio.CancelledError:
                    log.append("handler cancelled")
                    raise

            server = await asyncio.start_server(handle, "db", 1)
            try:
                await server.serve_forever()
            except asyncio.CancelledError:
                log.append("server cancelled")
                raise

        detangle.spawn(server_main(), host="db")
        await asyncio.sleep(0.1)
        reader, _ = await asyncio.open_connection("db", 1)
        await asyncio.sleep(0.1)
        net.crash("db")
        try:
            await reader.read()
        except ConnectionResetError:
            log.append(f"client reset at {detangle.now():.1f}s")
        assert not net.is_up("db")
        try:
            await asyncio.open_connection("db", 1)
        except (ConnectionError, OSError):
            log.append("refused while down")
        net.restart("db")
        return log

    log = detangle.run(main, net={"tcp_timeout": 5})
    assert "server cancelled" in log
    assert "handler cancelled" in log
    assert any(entry.startswith("client reset at 5.2") for entry in log)
    assert "refused while down" in log


def test_datagrams_can_be_lost_duplicated_and_reordered() -> None:
    async def main() -> list[bytes]:
        got: list[bytes] = []

        class Receiver(asyncio.DatagramProtocol):
            def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
                got.append(data)

        loop = asyncio.get_running_loop()
        await loop.create_datagram_endpoint(Receiver, local_addr=("b", 9000))
        sender, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol, remote_addr=("b", 9000)
        )
        for i in range(8):
            sender.sendto(str(i).encode())
        await asyncio.sleep(1)
        return got

    perfect = detangle.run(main)
    assert perfect == [str(i).encode() for i in range(8)]
    config = {"drop": 0.2, "duplicate": 0.2, "latency": (0.001, 0.1)}
    outcomes = [detangle.run(main, seed=s, net=config) for s in range(30)]
    assert any(len(o) < 8 for o in outcomes)  # losses
    assert any(len(o) != len(set(o)) for o in outcomes)  # duplicates
    assert any(o != sorted(o) for o in outcomes)  # reordering


def test_mailboxes_copy_messages() -> None:
    async def main() -> tuple[object, tuple[str, int], bool]:
        net = detangle.network()
        a = net.mailbox(1, host="a")
        b = net.mailbox(2, host="b")
        payload = {"items": [1, 2]}
        a.send(("b", 2), payload)
        payload["items"].append(3)  # must not affect the message in flight
        msg, src = await b.recv()
        return msg, src, msg is payload

    msg, src, same = detangle.run(main)
    assert msg == {"items": [1, 2]}
    assert src == ("a", 1)
    assert not same


def test_mailbox_partition_drops_messages() -> None:
    async def main() -> int:
        net = detangle.network()
        a = net.mailbox(1, host="a")
        b = net.mailbox(2, host="b")
        net.disconnect("a", "b")
        a.send(("b", 2), "lost")
        await asyncio.sleep(1)
        net.reconnect("a", "b")
        a.send(("b", 2), "delivered")
        await asyncio.sleep(1)
        return b.pending()

    assert detangle.run(main) == 1


def test_host_context_is_inherited() -> None:
    async def main() -> list[str]:
        from detangle._context import host_name

        seen: list[str] = []

        async def child() -> None:
            seen.append(host_name())

        async def parent() -> None:
            seen.append(host_name())
            await asyncio.create_task(child())

        await detangle.spawn(parent(), host="web-1")
        seen.append(host_name())
        return seen

    assert detangle.run(main) == ["web-1", "web-1", "localhost"]


def test_protocol_exception_is_reported() -> None:
    async def main() -> None:
        class Broken(asyncio.Protocol):
            def data_received(self, data: bytes) -> None:
                raise ValueError("parser crashed")

        await asyncio.get_running_loop().create_server(Broken, "srv", 1)
        _, writer = await asyncio.open_connection("srv", 1)
        writer.write(b"x")
        await asyncio.sleep(1)

    with pytest.raises(BugFound) as info:
        detangle.explore(main, runs=1)
    assert info.value.report.failure.kind == "callback-error"
    assert "parser crashed" in str(info.value)
