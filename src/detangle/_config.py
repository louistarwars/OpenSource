"""Simulation configuration."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any

__all__ = ["NetConfig", "SimConfig"]


@dataclass(frozen=True)
class NetConfig:
    """Behaviour of the simulated network.

    All durations are in virtual seconds.  Latencies are drawn from
    ``latency_steps`` evenly spaced values between ``latency[0]`` and
    ``latency[1]``; the minimum is the default (choice 0).
    """

    latency: tuple[float, float] = (0.0005, 0.005)
    latency_steps: int = 4
    #: Probability that a datagram / mailbox message is lost.
    drop: float = 0.0
    #: Probability that a datagram / mailbox message is delivered twice.
    duplicate: float = 0.0
    #: Probability that a TCP write is split into several reads on the peer.
    fragment: float = 0.0
    #: Maximum number of pieces a fragmented write is split into.
    max_fragments: int = 3
    #: Time before a connection attempt to an unreachable host fails.
    connect_timeout: float = 10.0
    #: Time a TCP connection survives a partition before being reset.
    tcp_timeout: float = 30.0
    #: Retransmission interval while a link is partitioned.
    retransmit_interval: float = 0.2
    #: Deep-copy mailbox messages (simulates serialisation, catches aliasing).
    copy_messages: bool = True

    def __post_init__(self) -> None:
        lo, hi = self.latency
        if lo < 0 or hi < lo:
            raise ValueError(f"invalid latency range {self.latency!r}")
        for name in ("drop", "duplicate", "fragment"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be a probability, got {value!r}")
        if self.latency_steps < 1 or self.max_fragments < 1:
            raise ValueError("latency_steps and max_fragments must be >= 1")


@dataclass(frozen=True)
class SimConfig:
    """Knobs controlling what a simulation explores and what counts as a bug."""

    #: Explore reorderings of ready tasks.  ``False`` keeps asyncio's strict
    #: FIFO order and only explores time/network/fault nondeterminism.
    reorder: bool = True
    #: Maximum extra delay (virtual seconds) a timer may fire late.
    timer_jitter: float = 0.0
    #: Number of distinct jitter values explored between 0 and ``timer_jitter``.
    jitter_steps: int = 4
    #: Virtual time consumed by every step (models CPU time; 0 = infinitely fast).
    step_cost: float = 0.0
    #: Abort the run (as a failure) after this many steps.
    max_steps: int = 200_000
    #: Abort the run (as a failure) when virtual time exceeds this value.
    max_time: float | None = None
    #: Exceptions in background tasks that nobody retrieved are failures.
    fail_on_unobserved: bool = True
    #: Exceptions escaping plain callbacks (``loop.call_soon`` & co) are failures.
    fail_on_callback_error: bool = True
    #: Tasks still pending when the main coroutine returns are failures.
    check_leaks: bool = False
    #: Seed the global :mod:`random` module from the schedule (restored after).
    seed_random: bool = True
    #: Patch ``time.time``/``time.monotonic``/``time.perf_counter`` to virtual time.
    patch_time: bool = False
    #: Network simulation parameters.
    net: NetConfig = field(default_factory=NetConfig)

    def __post_init__(self) -> None:
        if self.timer_jitter < 0 or self.step_cost < 0:
            raise ValueError("timer_jitter and step_cost must be >= 0")
        if self.jitter_steps < 2 and self.timer_jitter > 0:
            raise ValueError("jitter_steps must be >= 2 when timer_jitter > 0")
        if self.max_steps < 1:
            raise ValueError("max_steps must be >= 1")

    def replace(self, **changes: Any) -> SimConfig:
        return dataclasses.replace(self, **changes)


_SIM_FIELDS = {f.name for f in dataclasses.fields(SimConfig)}
_NET_FIELDS = {f.name for f in dataclasses.fields(NetConfig)}


def build_config(config: SimConfig | None, options: dict[str, Any]) -> SimConfig:
    """Merge keyword options (``timer_jitter=...``, ``net=...``) into a config."""
    base = config or SimConfig()
    sim_changes: dict[str, Any] = {}
    for key, value in options.items():
        if key not in _SIM_FIELDS:
            raise TypeError(f"unknown simulation option {key!r}")
        if key == "net" and isinstance(value, dict):
            bad = set(value) - _NET_FIELDS
            if bad:
                raise TypeError(f"unknown network option(s): {', '.join(sorted(bad))}")
            value = dataclasses.replace(base.net, **value)
        sim_changes[key] = value
    return base.replace(**sim_changes) if sim_changes else base
