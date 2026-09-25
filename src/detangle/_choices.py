"""Decisions, choice sequences and replay tokens.

Every source of nondeterminism in a simulation -- which ready task runs next,
how late a timer fires, how long a network packet takes, whether a fault is
injected -- is funnelled through a single primitive: *pick an integer in
``range(n)``*.  The value ``0`` always means "what asyncio would do by
default" (run the oldest ready callback, fire the timer on time, deliver the
packet with minimum latency, inject no fault).

A run is therefore fully described by the list of integers it picked.  That
list can be replayed, mutated, shrunk and serialised into a short *token*
which reproduces the run bit-for-bit.
"""

from __future__ import annotations

import base64
import zlib
from collections.abc import Iterable, Sequence
from typing import NamedTuple

__all__ = ["Decision", "decode_token", "encode_token"]

TOKEN_PREFIX = "dt1-"

#: Decision kinds.
SCHED = "sched"  # which ready lane runs next
TIMER = "timer"  # timer jitter bucket
NET = "net"  # network latency bucket / fragmentation
FAULT = "fault"  # injected failures (drops, duplicates, cancellations)
DATA = "data"  # user-level nondeterministic values (detangle.choice & co)
SEED = "seed"  # seed for the global ``random`` module


class Decision(NamedTuple):
    """One nondeterministic choice made during a run."""

    kind: str
    n: int
    value: int


def _write_varint(out: bytearray, value: int) -> None:
    if value < 0:
        raise ValueError("choice values must be non-negative")
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return


def _read_varints(data: bytes) -> list[int]:
    values: list[int] = []
    shift = 0
    current = 0
    for byte in data:
        current |= (byte & 0x7F) << shift
        if byte & 0x80:
            shift += 7
            if shift > 70:
                raise ValueError("corrupt detangle token (varint too long)")
        else:
            values.append(current)
            current = 0
            shift = 0
    if shift:
        raise ValueError("corrupt detangle token (truncated varint)")
    return values


def strip_trailing_zeros(values: Sequence[int]) -> list[int]:
    """Trailing zeros are implicit: replaying past the end picks 0."""
    end = len(values)
    while end and values[end - 1] == 0:
        end -= 1
    return list(values[:end])


def encode_token(values: Iterable[int]) -> str:
    """Serialise a choice sequence into a compact, URL/shell-safe token."""
    raw = bytearray()
    for v in strip_trailing_zeros(list(values)):
        _write_varint(raw, v)
    compressed = zlib.compress(bytes(raw), 9)
    payload = b"z" + compressed if len(compressed) < len(raw) else b"r" + bytes(raw)
    text = base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")
    return TOKEN_PREFIX + text


def decode_token(token: str) -> list[int]:
    """Inverse of :func:`encode_token`."""
    token = token.strip()
    if not token.startswith(TOKEN_PREFIX):
        raise ValueError(f"not a detangle token (expected prefix {TOKEN_PREFIX!r}): {token!r}")
    text = token[len(TOKEN_PREFIX) :]
    padded = text + "=" * (-len(text) % 4)
    try:
        payload = base64.urlsafe_b64decode(padded.encode("ascii"))
    except ValueError as exc:  # binascii.Error is a ValueError
        raise ValueError(f"corrupt detangle token: {exc}") from None
    if not payload:
        raise ValueError("corrupt detangle token (empty payload)")
    tag, body = payload[:1], payload[1:]
    if tag == b"z":
        try:
            body = zlib.decompress(body)
        except zlib.error as exc:
            raise ValueError(f"corrupt detangle token: {exc}") from None
    elif tag != b"r":
        raise ValueError("corrupt detangle token (unknown encoding)")
    return _read_varints(body)


def complexity(values: Sequence[int]) -> tuple[int, int, int]:
    """Ordering used by the shrinker: fewer deviations, then smaller, then shorter."""
    nonzero = sum(1 for v in values if v)
    return (nonzero, sum(min(v, 1 << 20) for v in values), len(strip_trailing_zeros(values)))
