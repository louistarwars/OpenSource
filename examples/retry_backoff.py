"""Virtual time: test hours of retries and timeouts in milliseconds.

``detangle.run`` is a drop-in ``asyncio.run`` whose clock is virtual: sleeps
return instantly while ``loop.time()`` advances exactly as it would in real
life.  Great for retry policies, rate limiters, TTL caches, heartbeats...

    python examples/retry_backoff.py
"""

from __future__ import annotations

import asyncio
import time

import detangle


class FlakyService:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls: list[float] = []

    async def call(self) -> str:
        self.calls.append(asyncio.get_running_loop().time())
        if len(self.calls) <= self.failures:
            raise ConnectionError("service unavailable")
        return "ok"


async def with_backoff(service: FlakyService, base: float = 1.0, cap: float = 600.0) -> str:
    delay = base
    while True:
        try:
            return await service.call()
        except ConnectionError:
            await asyncio.sleep(delay)
            delay = min(delay * 2, cap)


async def check_backoff_schedule() -> list[float]:
    service = FlakyService(failures=12)
    assert await with_backoff(service) == "ok"
    gaps = [b - a for a, b in zip(service.calls, service.calls[1:])]
    assert gaps == [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 600, 600], gaps
    return service.calls


def demo() -> str:
    start = time.perf_counter()
    calls = detangle.run(check_backoff_schedule)
    elapsed = time.perf_counter() - start
    return (
        f"{len(calls)} attempts spanning {calls[-1] / 3600:.2f} hours of virtual time, "
        f"checked in {elapsed * 1000:.1f} ms of real time"
    )


if __name__ == "__main__":
    print(demo())
