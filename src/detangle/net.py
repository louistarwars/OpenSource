"""A deterministic, adversarial, in-memory network.

Inside a simulation, the standard asyncio networking APIs are served by this
module instead of the operating system:

* ``asyncio.open_connection`` / ``asyncio.start_server`` and
  ``loop.create_connection`` / ``loop.create_server`` (TCP-like streams:
  reliable and ordered, but with variable latency, write coalescing and --
  optionally -- fragmentation of writes into several reads);
* ``loop.create_datagram_endpoint`` (UDP-like: loss, duplication,
  reordering);
* :class:`Mailbox` -- the same, for Python objects (handy to prototype
  distributed protocols such as Raft or Paxos).

Every simulated host has a name (``"db"``, ``"10.0.0.2"``...).  Code runs
on the host set by :func:`detangle.spawn(coro, host=...)
<detangle.spawn>`; ``"localhost"`` by default.  The network can be
partitioned, healed, and hosts can crash and restart -- all under the
control of your test, and all perfectly reproducible.

Latencies, drops, duplicates and fragmentation are *decisions* explored by
the strategy (the default being: minimum latency, no fault), so they are
shrunk too: a failing test ends up with only the faults it really needs.
"""

from __future__ import annotations

import asyncio
import collections
import contextvars
import copy
import itertools
import socket
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

from ._choices import FAULT, NET
from ._config import NetConfig
from ._context import DEFAULT_HOST, current_host, host_name
from .errors import RealIOError, SimulationError

if TYPE_CHECKING:
    from ._loop import SimLoop

__all__ = ["Mailbox", "SimDatagramTransport", "SimNetwork", "SimServer", "SimTransport"]

Address = tuple[str, int]

_LOOPBACK = {"localhost", "127.0.0.1", "::1", "ip6-localhost"}
_WILDCARD = {"", "0.0.0.0", "::"}


def _preview(data: bytes, limit: int = 24) -> str:
    text = repr(data[:limit])
    if len(data) > limit:
        text += "..."
    return text


def _fmt(addr: Address) -> str:
    return f"{addr[0]}:{addr[1]}"


class _FakeSocket:
    """Enough of a socket for ``server.sockets[0].getsockname()`` & friends."""

    def __init__(self, sockname: Address, peername: Address | None, kind: int) -> None:
        self._sockname = sockname
        self._peername = peername
        self.family = socket.AF_INET
        self.type = kind
        self.proto = 0

    def getsockname(self) -> Address:
        return self._sockname

    def getpeername(self) -> Address:
        if self._peername is None:
            raise OSError(107, "Transport endpoint is not connected")
        return self._peername

    def fileno(self) -> int:
        return -1

    def setsockopt(self, *args: Any) -> None:
        pass

    def getsockopt(self, *args: Any) -> int:
        return 0

    def __repr__(self) -> str:
        return f"<detangle simulated socket {_fmt(self._sockname)}>"


# ----------------------------------------------------------------------------
# TCP
# ----------------------------------------------------------------------------


class _Pipe:
    """One direction of a simulated TCP connection: ordered, reliable, delayed."""

    def __init__(
        self, net: SimNetwork, conn: _Connection, src: SimTransport, dst: SimTransport
    ) -> None:
        self.net = net
        self.conn = conn
        self.src = src
        self.dst = dst
        self.queue: collections.deque[tuple[float, str, bytes]] = collections.deque()
        self.last = 0.0
        self.blocked_since: float | None = None
        self.pump: asyncio.TimerHandle | None = None

    def send(self, kind: str, payload: bytes = b"") -> None:
        loop = self.net.loop
        when = max(loop.time() + self.net.latency(), self.last)
        self.last = when
        self.queue.append((when, kind, payload))
        if self.pump is None:
            self._schedule(when)

    def clear(self) -> None:
        self.queue.clear()
        if self.pump is not None:
            self.pump.cancel()
            self.pump = None

    def _schedule(self, when: float) -> None:
        self.pump = self.net.loop.call_at_exact(when, self._run, context=self.dst._ctx)

    def _run(self) -> None:
        self.pump = None
        net = self.net
        now = net.loop.time()
        queue = self.queue
        while queue:
            when, kind, payload = queue[0]
            if when > now:
                self._schedule(when)
                return
            dst = self.dst
            if dst._closed:
                queue.clear()
                return
            if not net.can_deliver(self.src.host, dst.host):
                if self.blocked_since is None:
                    self.blocked_since = now
                if now - self.blocked_since >= net.config.tcp_timeout:
                    self.conn.reset(TimeoutError(110, "Connection timed out (network partition)"))
                    return
                self._schedule(now + net.config.retransmit_interval)
                return
            self.blocked_since = None
            queue.popleft()
            if kind == "data":
                # Everything already buffered by the kernel is read at once.
                chunks = [payload]
                while queue and queue[0][0] <= now and queue[0][1] == "data":
                    chunks.append(queue.popleft()[2])
                payload = b"".join(chunks)
                route = f"{_fmt(self.src.sockname)} -> {_fmt(dst.sockname)}"
                net._trace(f"{route} {len(payload)}B {_preview(payload)}")
            elif kind == "eof":
                net._trace(f"{_fmt(self.src.sockname)} -> {_fmt(dst.sockname)} FIN")
            dst._receive(kind, payload)


class _Connection:
    def __init__(self, net: SimNetwork, client: SimTransport, server: SimTransport) -> None:
        self.net = net
        self.client = client
        self.server = server
        self.c2s = _Pipe(net, self, client, server)
        self.s2c = _Pipe(net, self, server, client)
        client._out = self.c2s
        server._out = self.s2c
        client._conn = self
        server._conn = self

    def sides(self) -> tuple[SimTransport, SimTransport]:
        return (self.client, self.server)

    def reset(self, exc: BaseException) -> None:
        self.net._trace(
            f"connection {_fmt(self.client.sockname)} <-> {_fmt(self.server.sockname)} reset: {exc}"
        )
        self.c2s.clear()
        self.s2c.clear()
        for side in self.sides():
            if not side._closed:
                self.net.loop.call_soon(side._lost, exc, context=side._ctx)

    def closed(self) -> bool:
        return self.client._closed and self.server._closed


class SimTransport(asyncio.Transport):
    """Transport of a simulated TCP connection."""

    def __init__(
        self,
        net: SimNetwork,
        host: str,
        sockname: Address,
        peername: Address,
        ctx: contextvars.Context,
    ) -> None:
        super().__init__()
        self._net = net
        self.host = host
        self.sockname = sockname
        self.peername = peername
        self._ctx = ctx
        self._protocol: Any = None
        self._out: _Pipe | None = None
        self._conn: _Connection | None = None
        self._closing = False
        self._closed = False
        self._paused = False
        self._eof_sent = False
        self._pending: list[tuple[str, bytes]] = []
        self._flush_scheduled = False
        self._limits = (16 * 1024, 64 * 1024)
        self._server: SimServer | None = None
        self._extra = {
            "peername": peername,
            "sockname": sockname,
            "socket": _FakeSocket(sockname, peername, socket.SOCK_STREAM),
        }

    def __repr__(self) -> str:
        state = "closed" if self._closed else "closing" if self._closing else "open"
        return f"<SimTransport {_fmt(self.sockname)} -> {_fmt(self.peername)} {state}>"

    # -- asyncio.BaseTransport ---------------------------------------------------

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return self._extra.get(name, default)

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        if not self._eof_sent and self._out is not None:
            self._eof_sent = True
            self._out.send("eof")
        self._net.loop.call_soon(self._lost, None, context=self._ctx)

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        self._protocol = protocol

    def get_protocol(self) -> Any:
        return self._protocol

    # -- asyncio.ReadTransport ----------------------------------------------------

    def is_reading(self) -> bool:
        return not self._paused and not self._closing

    def pause_reading(self) -> None:
        self._paused = True

    def resume_reading(self) -> None:
        if not self._paused:
            return
        self._paused = False
        self._schedule_flush()

    # -- asyncio.WriteTransport ---------------------------------------------------

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        high = 64 * 1024 if high is None else high
        low = high // 4 if low is None else low
        self._limits = (low, high)

    def get_write_buffer_limits(self) -> tuple[int, int]:
        return self._limits

    def get_write_buffer_size(self) -> int:
        return 0

    def write(self, data: bytes | bytearray | memoryview) -> None:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(
                f"data argument must be a bytes-like object, not {type(data).__name__!r}"
            )
        if self._eof_sent and not self._closing:
            raise RuntimeError("Cannot call write() after write_eof()")
        if self._closing or self._closed or self._out is None or not data:
            return
        for piece in self._net._fragment(bytes(data)):
            self._out.send("data", piece)

    def write_eof(self) -> None:
        if self._closing or self._eof_sent:
            return
        self._eof_sent = True
        if self._out is not None:
            self._out.send("eof")

    def can_write_eof(self) -> bool:
        return True

    def abort(self) -> None:
        if self._closed:
            return
        self._closing = True
        if self._out is not None:
            self._out.clear()
            self._out.send("rst")
        self._net.loop.call_soon(self._lost, None, context=self._ctx)

    # -- simulation internals --------------------------------------------------------

    def _attach(self, protocol: Any) -> None:
        self._protocol = protocol
        protocol.connection_made(self)
        if self._pending:
            self._schedule_flush()

    def _schedule_flush(self) -> None:
        if not self._flush_scheduled and self._pending:
            self._flush_scheduled = True
            self._net.loop.call_soon(self._flush, context=self._ctx)

    def _flush(self) -> None:
        self._flush_scheduled = False
        while (
            self._pending and not self._paused and not self._closed and self._protocol is not None
        ):
            kind, payload = self._pending.pop(0)
            self._deliver(kind, payload)

    def _receive(self, kind: str, payload: bytes) -> None:
        if self._closed:
            return
        if kind == "rst":
            self._lost(ConnectionResetError(104, "Connection reset by peer"))
            return
        if self._protocol is None or self._paused or self._pending:
            self._pending.append((kind, payload))
            if self._protocol is not None and not self._paused:
                self._schedule_flush()
            return
        self._deliver(kind, payload)

    def _deliver(self, kind: str, payload: bytes) -> None:
        if kind == "data":
            if self._closing:
                return
            self._protocol.data_received(payload)
        elif kind == "eof":
            keep_open = self._protocol.eof_received()
            if not keep_open:
                self.close()

    def _lost(self, exc: BaseException | None) -> None:
        if self._closed:
            return
        self._closed = True
        self._closing = True
        self._pending.clear()
        if self._server is not None:
            self._server._detach(self)
        if self._protocol is not None:
            self._protocol.connection_lost(exc)

    def _kill(self) -> None:
        """Host crash: vanish without telling anybody."""
        self._closed = True
        self._closing = True
        self._pending.clear()
        if self._out is not None:
            self._out.clear()
        if self._server is not None:
            self._server._detach(self)


class SimServer(asyncio.AbstractServer):
    """A listening socket on the simulated network."""

    def __init__(
        self,
        net: SimNetwork,
        address: Address,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        ctx: contextvars.Context,
        serving: bool,
    ) -> None:
        self._net = net
        self.address = address
        self._factory = protocol_factory
        self._ctx = ctx
        self._serving = serving
        self._closed = False
        self._transports: list[SimTransport] = []
        self._waiters: list[asyncio.Future[None]] = []
        self._serve_forever_fut: asyncio.Future[None] | None = None

    def __repr__(self) -> str:
        return f"<SimServer {_fmt(self.address)}{' closed' if self._closed else ''}>"

    @property
    def sockets(self) -> tuple[_FakeSocket, ...]:
        if self._closed:
            return ()
        return (_FakeSocket(self.address, None, socket.SOCK_STREAM),)

    def get_loop(self) -> asyncio.AbstractEventLoop:
        return self._net.loop

    def is_serving(self) -> bool:
        return self._serving and not self._closed

    async def start_serving(self) -> None:
        if not self._closed:
            self._serving = True

    async def serve_forever(self) -> None:
        if self._serve_forever_fut is not None:
            raise RuntimeError(f"server {self!r} is already being awaited on serve_forever()")
        if self._closed:
            raise RuntimeError(f"server {self!r} is closed")
        self._serving = True
        self._serve_forever_fut = self._net.loop.create_future()
        try:
            await self._serve_forever_fut
        except asyncio.CancelledError:
            try:
                self.close()
                await self.wait_closed()
            finally:
                raise
        finally:
            self._serve_forever_fut = None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._serving = False
        self._net._unlisten(self)
        if self._serve_forever_fut is not None and not self._serve_forever_fut.done():
            self._serve_forever_fut.cancel()
        self._wake()

    def close_clients(self) -> None:
        for transport in list(self._transports):
            transport.close()

    def abort_clients(self) -> None:
        for transport in list(self._transports):
            transport.abort()

    async def wait_closed(self) -> None:
        while not (self._closed and not self._transports):
            waiter: asyncio.Future[None] = self._net.loop.create_future()
            self._waiters.append(waiter)
            await waiter

    def _detach(self, transport: SimTransport) -> None:
        if transport in self._transports:
            self._transports.remove(transport)
        self._wake()

    def _wake(self) -> None:
        if self._closed and not self._transports:
            waiters, self._waiters = self._waiters, []
            for waiter in waiters:
                if not waiter.done():
                    waiter.set_result(None)


# ----------------------------------------------------------------------------
# UDP and mailboxes
# ----------------------------------------------------------------------------


class SimDatagramTransport(asyncio.DatagramTransport):
    """Transport of a simulated UDP socket."""

    def __init__(
        self, net: SimNetwork, address: Address, remote: Address | None, ctx: contextvars.Context
    ) -> None:
        super().__init__()
        self._net = net
        self.address = address
        self.host = address[0]
        self._remote = remote
        self._ctx = ctx
        self._protocol: Any = None
        self._closing = False
        self._extra = {
            "sockname": address,
            "peername": remote,
            "socket": _FakeSocket(address, remote, socket.SOCK_DGRAM),
        }

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return self._extra.get(name, default)

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._net._datagram.pop(self.address, None)
        if self._protocol is not None:
            self._net.loop.call_soon(self._protocol.connection_lost, None, context=self._ctx)

    def abort(self) -> None:
        self.close()

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        self._protocol = protocol

    def get_protocol(self) -> Any:
        return self._protocol

    def get_write_buffer_size(self) -> int:
        return 0

    def sendto(self, data: bytes | bytearray | memoryview, addr: Any = None) -> None:
        if self._closing:
            return
        target = addr if addr is not None else self._remote
        if target is None:
            raise ValueError("no destination address: pass addr or use remote_addr")
        dst = self._net._resolve_address(target)
        self._net._send_datagram(self.address, dst, bytes(data), self._net._deliver_datagram)

    def _deliver(self, data: bytes, src: Address) -> None:
        if not self._closing and self._protocol is not None:
            self._protocol.datagram_received(data, src)


class Mailbox:
    """An unreliable, unordered message socket carrying Python objects.

    Messages may be delayed, reordered, dropped or duplicated according to the
    network configuration and partitions.  They are deep-copied on send (as if
    serialised), so sender and receiver never share mutable state.
    """

    def __init__(self, net: SimNetwork, address: Address) -> None:
        self._net = net
        self.address = address
        self.host = address[0]
        self._queue: collections.deque[tuple[Any, Address]] = collections.deque()
        self._waiters: collections.deque[asyncio.Future[None]] = collections.deque()
        self._closed = False

    def __repr__(self) -> str:
        return f"<Mailbox {_fmt(self.address)}>"

    def send(self, to: Address | str, message: Any) -> None:
        """Send *message* to the mailbox at *to* (fire and forget)."""
        if self._closed:
            raise SimulationError(f"{self!r} is closed")
        dst = self._net._resolve_address(to)
        if self._net.config.copy_messages:
            message = copy.deepcopy(message)
        self._net._send_datagram(self.address, dst, message, self._net._deliver_message)

    async def recv(self) -> tuple[Any, Address]:
        """Wait for the next message; returns ``(message, sender_address)``."""
        while not self._queue:
            if self._closed:
                raise SimulationError(f"{self!r} is closed")
            waiter: asyncio.Future[None] = self._net.loop.create_future()
            self._waiters.append(waiter)
            try:
                await waiter
            finally:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)
        return self._queue.popleft()

    def recv_nowait(self) -> tuple[Any, Address] | None:
        return self._queue.popleft() if self._queue else None

    def pending(self) -> int:
        return len(self._queue)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._net._mailboxes.pop(self.address, None)
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(None)

    def _deliver(self, message: Any, src: Address) -> None:
        if self._closed:
            return
        self._queue.append((message, src))
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                break


# ----------------------------------------------------------------------------
# The network itself
# ----------------------------------------------------------------------------


class SimNetwork:
    """The simulated network of one run (get it with :func:`detangle.network`)."""

    def __init__(self, loop: SimLoop, config: NetConfig) -> None:
        self.loop = loop
        self.config = config
        self._listeners: dict[Address, SimServer] = {}
        self._datagram: dict[Address, SimDatagramTransport] = {}
        self._mailboxes: dict[Address, Mailbox] = {}
        self._connections: list[_Connection] = []
        self._cut: set[frozenset[str]] = set()
        self._down: set[str] = set()
        self._hosts: dict[str, list[asyncio.Task[Any]]] = {DEFAULT_HOST: []}
        self._next_port = 40000
        self._neutral = contextvars.Context()

    # -- topology -----------------------------------------------------------------

    @property
    def hosts(self) -> list[str]:
        """Hosts known so far (in order of first appearance)."""
        return list(self._hosts)

    def ensure_host(self, host: str) -> None:
        self._hosts.setdefault(host, [])

    def is_up(self, host: str) -> bool:
        return host not in self._down

    def reachable(self, a: str, b: str) -> bool:
        """Can packets currently flow between *a* and *b*?"""
        return a == b or frozenset((a, b)) not in self._cut

    def can_deliver(self, src: str, dst: str) -> bool:
        return self.reachable(src, dst) and src not in self._down and dst not in self._down

    def partition(self, *groups: Iterable[str] | str) -> None:
        """Split the network: hosts in different groups cannot talk to each other.

        ``net.partition(["a"], ["b", "c"])`` isolates ``a`` from ``b`` and
        ``c``.  Hosts not mentioned keep talking to everybody.  Partitions
        accumulate until :meth:`heal`.
        """
        sets = [{g} if isinstance(g, str) else set(g) for g in groups]
        for i, left in enumerate(sets):
            for right in sets[i + 1 :]:
                for a in left:
                    for b in right:
                        if a != b:
                            self._cut.add(frozenset((a, b)))
        for group in sets:
            for host in group:
                self.ensure_host(host)
        self._trace("partition " + " | ".join("{" + ", ".join(sorted(g)) + "}" for g in sets))

    def disconnect(self, a: str, b: str) -> None:
        """Cut the link between two hosts (both directions)."""
        self._cut.add(frozenset((a, b)))
        self._trace(f"link {a} <-> {b} cut")

    def reconnect(self, a: str, b: str) -> None:
        self._cut.discard(frozenset((a, b)))
        self._trace(f"link {a} <-> {b} restored")

    def heal(self) -> None:
        """Remove every partition and cut link."""
        self._cut.clear()
        self._trace("network healed")

    def crash(self, host: str) -> None:
        """Crash *host*: its tasks are cancelled, its sockets vanish silently.

        Peers are not told: packets to the host are lost, and their
        connections are reset after ``NetConfig.tcp_timeout`` (as TCP
        keep-alive / retransmission timeouts would).
        """
        if host in self._down:
            return
        self._down.add(host)
        self.ensure_host(host)
        self._trace(f"host {host} crashed")
        for address, server in list(self._listeners.items()):
            if address[0] == host:
                server.close()
        for address, dgram in list(self._datagram.items()):
            if address[0] == host:
                dgram._closing = True
                del self._datagram[address]
        for address, mailbox in list(self._mailboxes.items()):
            if address[0] == host:
                mailbox.close()
        timeout = self.config.tcp_timeout
        for conn in list(self._connections):
            for side, other in (conn.sides(), conn.sides()[::-1]):
                if side.host == host and not side._closed:
                    side._kill()
                    if not other._closed:
                        exc = ConnectionResetError(104, f"Connection reset (host {host} crashed)")
                        self.loop.call_at_exact(
                            self.loop.time() + timeout, other._lost, exc, context=other._ctx
                        )
        for task in self._hosts.get(host, []):
            if not task.done():
                task.cancel(msg=f"detangle: host {host} crashed")
        self._hosts[host] = []

    def restart(self, host: str) -> None:
        """Bring a crashed host back (start its processes again with :func:`detangle.spawn`)."""
        if host in self._down:
            self._down.discard(host)
            self._trace(f"host {host} restarted")

    # -- randomness ----------------------------------------------------------------

    def latency(self) -> float:
        lo, hi = self.config.latency
        steps = self.config.latency_steps
        if hi <= lo or steps <= 1:
            return lo
        k = self.loop.choose(steps, NET)
        return lo + (hi - lo) * k / (steps - 1)

    def _fragment(self, data: bytes) -> list[bytes]:
        config = self.config
        if len(data) < 2 or config.fragment <= 0 or config.max_fragments < 2:
            return [data]
        if not self.loop.flip(config.fragment, NET):
            return [data]
        pieces = 1 + self.loop.choose(min(config.max_fragments, len(data)) - 1, NET) + 1
        pieces = min(pieces, len(data))
        cuts: list[int] = []
        start = 0
        for i in range(pieces - 1):
            remaining = len(data) - start - (pieces - 1 - i)
            size = 1 + self.loop.choose(max(1, remaining), NET)
            start += size
            cuts.append(start)
        bounds = [0, *cuts, len(data)]
        pieces_list = [data[a:b] for a, b in itertools.pairwise(bounds) if a < b]
        if len(pieces_list) > 1:
            sizes = "+".join(str(len(p)) for p in pieces_list)
            self._trace(f"write of {len(data)}B fragmented into {sizes}B")
        return pieces_list

    # -- helpers ------------------------------------------------------------------

    def _trace(self, text: str) -> None:
        tracer = self.loop.tracer
        if tracer is not None:
            tracer.on_net("net", text)

    def _on_task_created(self, task: asyncio.Task[Any], host: str | None) -> None:
        tasks = self._hosts.setdefault(host or DEFAULT_HOST, [])
        if len(tasks) > 64:
            tasks[:] = [t for t in tasks if not t.done()]
        tasks.append(task)

    def _alloc_port(self) -> int:
        self._next_port += 1
        return self._next_port

    def _context_for(self, host: str) -> contextvars.Context:
        ctx = contextvars.copy_context()
        ctx.run(current_host.set, host)
        return ctx

    def _resolve_host(self, host: Any) -> str:
        if host is None:
            return host_name()
        if isinstance(host, bytes):
            host = host.decode()
        host = str(host)
        if host in _LOOPBACK or host in _WILDCARD:
            return host_name()
        return str(host)

    def _resolve_address(self, address: Any) -> Address:
        if isinstance(address, str):
            host, _, port = address.rpartition(":")
            return (self._resolve_host(host or None), int(port))
        return (self._resolve_host(address[0]), int(address[1]))

    async def _delay(self, seconds: float) -> None:
        future = self.loop.create_future()
        self.loop.call_at_exact(self.loop.time() + seconds, _set_result_if_pending, future)
        await future

    def _check_host_up(self, host: str) -> None:
        if host in self._down:
            raise OSError(113, f"No route to host (this host, {host}, has crashed)")

    # -- asyncio loop API --------------------------------------------------------------

    async def getaddrinfo(
        self,
        host: Any,
        port: Any,
        *,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[Any]:
        resolved = self._resolve_host(host)
        port_num = int(port) if port not in (None, "") else 0
        kinds = [type] if type else [socket.SOCK_STREAM, socket.SOCK_DGRAM]
        return [
            (socket.AF_INET, kind, 17 if kind == socket.SOCK_DGRAM else 6, "", (resolved, port_num))
            for kind in kinds
        ]

    async def create_server(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        host: Any = None,
        port: Any = None,
        *,
        sock: Any = None,
        start_serving: bool = True,
        **_ignored: Any,
    ) -> SimServer:
        if sock is not None:
            raise RealIOError("create_server(sock=...) uses a real socket; pass host/port instead")
        if isinstance(host, (list, tuple)):
            host = host[0] if host else None
        bind_host = self._resolve_host(host)
        self._check_host_up(bind_host)
        port_num = int(port) if port else self._alloc_port()
        address = (bind_host, port_num)
        if address in self._listeners:
            raise OSError(98, f"[Errno 98] address already in use: {_fmt(address)}")
        self.ensure_host(bind_host)
        server = SimServer(
            self, address, protocol_factory, self._context_for(bind_host), start_serving
        )
        self._listeners[address] = server
        self._trace(f"listening on {_fmt(address)}")
        return server

    def _unlisten(self, server: SimServer) -> None:
        if self._listeners.get(server.address) is server:
            del self._listeners[server.address]

    async def create_connection(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        host: Any = None,
        port: Any = None,
        *,
        sock: Any = None,
        local_addr: Any = None,
        **_ignored: Any,
    ) -> tuple[SimTransport, asyncio.BaseProtocol]:
        if sock is not None:
            raise RealIOError(
                "create_connection(sock=...) uses a real socket; pass host/port instead"
            )
        if port is None:
            raise ValueError("port is required")
        src = host_name()
        self._check_host_up(src)
        dst = self._resolve_host(host)
        address = (dst, int(port))
        client_addr = (src, self._alloc_port())
        client_ctx = contextvars.copy_context()
        self._trace(f"{_fmt(client_addr)} connecting to {_fmt(address)}")

        # SYN: wait until it gets through (or give up).
        waited = 0.0
        while True:
            delay = self.latency()
            await self._delay(delay)
            waited += delay
            if self.can_deliver(src, dst):
                break
            if waited >= self.config.connect_timeout:
                self._trace(f"connect {_fmt(client_addr)} -> {_fmt(address)} timed out")
                raise TimeoutError(110, f"Connect call failed {address!r} (host unreachable)")
            await self._delay(min(self.config.retransmit_interval, self.config.connect_timeout))
            waited += self.config.retransmit_interval
        self._check_host_up(src)
        server = self._listeners.get(address)
        if server is None or not server.is_serving():
            await self._delay(self.latency())
            self._trace(f"connect {_fmt(client_addr)} -> {_fmt(address)} refused")
            raise ConnectionRefusedError(111, f"Connect call failed {address!r}")

        client = SimTransport(self, src, client_addr, address, client_ctx)
        server_side = SimTransport(self, dst, address, client_addr, server._ctx)
        conn = _Connection(self, client, server_side)
        self._connections.append(conn)
        if len(self._connections) > 64:
            self._connections = [c for c in self._connections if not c.closed()]
        server_side._server = server
        server._transports.append(server_side)
        self._trace(f"{_fmt(address)} accepted {_fmt(client_addr)}")
        # The server learns about the connection now (accept)...
        self.loop.call_soon(_accept, server, server_side, context=server._ctx)
        # ...and the SYN-ACK travels back to the client.
        await self._delay(self.latency())
        protocol = protocol_factory()
        client._attach(protocol)
        return client, protocol

    async def create_datagram_endpoint(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        local_addr: Any = None,
        remote_addr: Any = None,
        *,
        sock: Any = None,
        **_ignored: Any,
    ) -> tuple[SimDatagramTransport, asyncio.BaseProtocol]:
        if sock is not None:
            raise RealIOError("create_datagram_endpoint(sock=...) uses a real socket")
        host = self._resolve_host(local_addr[0] if local_addr else None)
        self._check_host_up(host)
        port = int(local_addr[1]) if local_addr and local_addr[1] else self._alloc_port()
        address = (host, port)
        if address in self._datagram or address in self._mailboxes:
            raise OSError(98, f"[Errno 98] address already in use: {_fmt(address)}")
        remote = self._resolve_address(remote_addr) if remote_addr is not None else None
        transport = SimDatagramTransport(self, address, remote, contextvars.copy_context())
        protocol = protocol_factory()
        transport._protocol = protocol
        self._datagram[address] = transport
        self.ensure_host(host)
        protocol.connection_made(transport)
        return transport, protocol

    def mailbox(self, port: int | None = None, *, host: str | None = None) -> Mailbox:
        """Open a :class:`Mailbox` on the current host (or *host*)."""
        name = host or host_name()
        self._check_host_up(name)
        address = (name, port or self._alloc_port())
        if address in self._mailboxes or address in self._datagram:
            raise OSError(98, f"[Errno 98] address already in use: {_fmt(address)}")
        self.ensure_host(name)
        box = Mailbox(self, address)
        self._mailboxes[address] = box
        return box

    # -- datagram plumbing -----------------------------------------------------------

    def _send_datagram(
        self,
        src: Address,
        dst: Address,
        payload: Any,
        deliver: Callable[[Address, Address, Any], None],
    ) -> None:
        config = self.config
        if self.loop.flip(config.drop, FAULT):
            self._trace(f"{_fmt(src)} -> {_fmt(dst)} dropped")
            return
        copies = 2 if self.loop.flip(config.duplicate, FAULT) else 1
        for i in range(copies):
            when = self.loop.time() + self.latency()
            item = payload if i == 0 or not config.copy_messages else copy.deepcopy(payload)
            self.loop.call_at_exact(when, deliver, src, dst, item, context=self._neutral)

    def _deliver_datagram(self, src: Address, dst: Address, data: Any) -> None:
        if not self.can_deliver(src[0], dst[0]):
            return
        endpoint = self._datagram.get(dst)
        if endpoint is None:
            return
        self._trace(f"{_fmt(src)} -> {_fmt(dst)} datagram {len(data)}B {_preview(data)}")
        endpoint._ctx.run(endpoint._deliver, data, src)

    def _deliver_message(self, src: Address, dst: Address, message: Any) -> None:
        if not self.can_deliver(src[0], dst[0]):
            return
        box = self._mailboxes.get(dst)
        if box is None:
            return
        self._trace(f"{_fmt(src)} -> {_fmt(dst)} {_short(message)}")
        box._deliver(message, src)


def _short(message: Any, limit: int = 48) -> str:
    text = repr(message)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _set_result_if_pending(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


def _accept(server: SimServer, transport: SimTransport) -> None:
    if transport._closed:
        return
    protocol = server._factory()
    transport._attach(protocol)
