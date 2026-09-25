"""A bug your normal test suite will never see.

Two requests for the same order arrive back to back: "create" and "ship".
Each handler writes to the database, then publishes an event.  With a fake
database, plain ``asyncio.run`` always finishes the first request first, so
the test passes -- every time.  In production, database latency varies and
"shipped" is sometimes published before "created": the consumer crashes.

detangle explores the other orders and finds it immediately.

    python examples/order_events.py
"""

from __future__ import annotations

import asyncio

import detangle


class FakeDB:
    async def write(self, key: str, value: str) -> None:
        await asyncio.sleep(0.005)  # a round-trip to the database


class Consumer:
    """Downstream service: builds a view of orders from the event stream."""

    def __init__(self) -> None:
        self.orders: dict[int, str] = {}

    def handle(self, event: str, order_id: int) -> None:
        if event == "created":
            self.orders[order_id] = "created"
        elif event == "shipped":
            if order_id not in self.orders:
                raise KeyError(f"order {order_id} shipped before it was created")
            self.orders[order_id] = "shipped"


class OrderService:
    def __init__(self, consumer: Consumer) -> None:
        self.db = FakeDB()
        self.consumer = consumer

    async def create(self, order_id: int) -> None:
        await self.db.write(f"order:{order_id}", "created")
        self.consumer.handle("created", order_id)

    async def ship(self, order_id: int) -> None:
        await self.db.write(f"order:{order_id}", "shipped")
        self.consumer.handle("shipped", order_id)


async def test_order_lifecycle() -> None:
    consumer = Consumer()
    service = OrderService(consumer)
    # The client fires both requests without waiting (e.g. two HTTP calls).
    await asyncio.gather(service.create(42), service.ship(42))
    assert consumer.orders[42] == "shipped"


def demo() -> tuple[str, str]:
    asyncio.run(test_order_lifecycle())  # passes with plain asyncio...
    detangle.run(test_order_lifecycle)  # ...and in detangle's default (asyncio) order
    plain = "no bug found with plain asyncio.run()"
    try:
        detangle.explore(test_order_lifecycle, runs=100, seed=1, database=False)
        return "no bug found (unexpected)", plain
    except detangle.BugFound as bug:
        return bug.report.render(), plain


if __name__ == "__main__":
    found, plain = demo()
    print(plain)
    print()
    print(found)
