"""Cancellation safety: timeouts that strike at the worst possible await.

``asyncio.wait_for`` / ``asyncio.timeout`` cancel your coroutine at whatever
``await`` it happens to be suspended on.  Code that is not written for that
leaks locks, connections and half-applied updates.  ``detangle.maybe_timeout``
lets the explorer choose *where* the timeout hits.

    python examples/connection_pool.py
"""

from __future__ import annotations

import asyncio

import detangle


class Pool:
    def __init__(self, size: int) -> None:
        self.size = size
        self.available = asyncio.Semaphore(size)
        self.in_use = 0

    async def acquire(self) -> None:
        await self.available.acquire()
        self.in_use += 1

    def release(self) -> None:
        self.in_use -= 1
        self.available.release()


async def query_unsafe(pool: Pool) -> str:
    await pool.acquire()
    await asyncio.sleep(0.01)  # run the query
    pool.release()  # never reached if cancelled during the query
    return "row"


async def query_safe(pool: Pool) -> str:
    await pool.acquire()
    try:
        await asyncio.sleep(0.01)
        return "row"
    finally:
        pool.release()


async def scenario(query) -> None:  # type: ignore[no-untyped-def]
    pool = Pool(size=2)

    async def client() -> None:
        try:
            await detangle.maybe_timeout(query(pool))
        except TimeoutError:
            pass  # the web framework would answer 504 here

    await asyncio.gather(*(client() for _ in range(4)))
    assert pool.in_use == 0, f"{pool.in_use} connection(s) leaked"


async def buggy() -> None:
    await scenario(query_unsafe)


async def fixed() -> None:
    await scenario(query_safe)


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
