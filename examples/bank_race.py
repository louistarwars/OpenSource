"""A check-then-act race across an ``await`` -- the most common asyncio bug.

python examples/bank_race.py
"""

from __future__ import annotations

import asyncio

import detangle


class Bank:
    def __init__(self) -> None:
        self.balances = {"alice": 100, "bob": 0}
        self.lock = asyncio.Lock()

    async def fraud_check(self, amount: int) -> None:
        await asyncio.sleep(0.01)  # a network call in real life

    async def transfer(self, src: str, dst: str, amount: int) -> None:
        if self.balances[src] < amount:  # check...
            raise ValueError("insufficient funds")
        await self.fraud_check(amount)  # ...other tasks run here...
        self.balances[src] -= amount  # ...act on stale information
        self.balances[dst] += amount

    async def safe_transfer(self, src: str, dst: str, amount: int) -> None:
        async with self.lock:
            if self.balances[src] < amount:
                raise ValueError("insufficient funds")
            await self.fraud_check(amount)
            self.balances[src] -= amount
            self.balances[dst] += amount


async def scenario(transfer_name: str) -> None:
    bank = Bank()
    transfer = getattr(bank, transfer_name)
    await asyncio.gather(
        transfer("alice", "bob", 60),
        transfer("alice", "bob", 60),
        return_exceptions=True,
    )
    assert bank.balances["alice"] >= 0, f"overdraft! {bank.balances}"


async def buggy() -> None:
    await scenario("transfer")


async def fixed() -> None:
    await scenario("safe_transfer")


def demo() -> tuple[str, str]:
    try:
        detangle.explore(buggy, runs=100, seed=1, database=False)
        found = "no bug found (unexpected)"
    except detangle.BugFound as bug:
        found = bug.report.render()
    # DFS gives a *guarantee*: every schedule with up to 3 deviations passes.
    stats = detangle.explore(fixed, runs=100_000, strategy="dfs:3", database=False)
    return found, stats.summary()


if __name__ == "__main__":
    found, proof = demo()
    print(found)
    print()
    print(proof)
