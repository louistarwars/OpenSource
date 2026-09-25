"""Invariants checked after every step: the thundering-herd cache.

``detangle.invariant`` registers a condition that must hold after *every*
scheduling step -- not just at the end of the test.

    python examples/cache_stampede.py
"""

from __future__ import annotations

import asyncio

import detangle


class Backend:
    def __init__(self) -> None:
        self.in_flight = 0
        self.calls = 0

    async def fetch(self, key: str) -> str:
        self.in_flight += 1
        self.calls += 1
        await asyncio.sleep(detangle.uniform(0.01, 0.05))  # slow and variable
        self.in_flight -= 1
        return f"value-of-{key}"


class NaiveCache:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self.data: dict[str, str] = {}

    async def get(self, key: str) -> str:
        if key not in self.data:
            self.data[key] = await self.backend.fetch(key)
        return self.data[key]


class CoalescingCache:
    """Concurrent misses for the same key share a single backend call."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self.data: dict[str, asyncio.Task[str]] = {}

    async def get(self, key: str) -> str:
        task = self.data.get(key)
        if task is None:
            task = self.data[key] = asyncio.ensure_future(self.backend.fetch(key))
        return await task


async def scenario(cache_cls) -> None:  # type: ignore[no-untyped-def]
    backend = Backend()
    cache = cache_cls(backend)
    detangle.invariant(lambda: backend.in_flight <= 1, "at most one backend call in flight")

    async def request(delay: float) -> None:
        await asyncio.sleep(delay)
        assert await cache.get("home") == "value-of-home"

    await asyncio.gather(*(request(detangle.uniform(0, 0.1)) for _ in range(5)))


async def buggy() -> None:
    await scenario(NaiveCache)


async def fixed() -> None:
    await scenario(CoalescingCache)


def demo() -> tuple[str, str]:
    try:
        detangle.explore(buggy, runs=300, seed=1, database=False)
        found = "no bug found (unexpected)"
    except detangle.BugFound as bug:
        found = bug.report.render(trace_limit=20)
    stats = detangle.explore(fixed, runs=300, seed=1, database=False)
    return found, stats.summary()


if __name__ == "__main__":
    found, check = demo()
    print(found)
    print()
    print(check)
