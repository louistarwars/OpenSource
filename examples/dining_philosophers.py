"""Deadlock detection with a readable wait-for graph.

python examples/dining_philosophers.py
"""

from __future__ import annotations

import asyncio

import detangle

N = 5


async def philosopher(i: int, forks: list[asyncio.Lock], ordered: bool) -> None:
    left, right = forks[i], forks[(i + 1) % N]
    first, second = left, right
    if ordered:
        # Always grab the lower-numbered fork first: breaks the cycle.
        first, second = sorted((left, right), key=forks.index)
    for _ in range(2):
        async with first:
            await asyncio.sleep(detangle.uniform(0, 0.01))  # think a little
            async with second:
                await asyncio.sleep(0.01)  # eat


async def dinner(ordered: bool) -> None:
    forks = [asyncio.Lock() for _ in range(N)]
    await asyncio.gather(*(philosopher(i, forks, ordered) for i in range(N)))


async def buggy() -> None:
    await dinner(ordered=False)


async def fixed() -> None:
    await dinner(ordered=True)


def demo() -> tuple[str, str]:
    try:
        detangle.explore(buggy, runs=500, seed=1, database=False)
        found = "no deadlock found (unexpected)"
    except detangle.BugFound as bug:
        found = bug.report.render(trace_limit=15)
    stats = detangle.explore(fixed, runs=500, seed=1, database=False)
    return found, stats.summary()


if __name__ == "__main__":
    found, check = demo()
    print(found)
    print()
    print(check)
