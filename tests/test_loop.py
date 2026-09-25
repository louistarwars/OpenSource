"""SimLoop must behave like a real asyncio loop (in FIFO mode), only faster."""

from __future__ import annotations

import asyncio
import contextvars
import sys
import time

import pytest

import detangle
from detangle import SimLoop


def test_virtual_time_is_instant() -> None:
    async def main() -> float:
        loop = asyncio.get_running_loop()
        start = loop.time()
        await asyncio.sleep(3600)
        await asyncio.sleep(86400 * 365)
        return loop.time() - start

    real = time.monotonic()
    elapsed = detangle.run(main)
    assert elapsed == pytest.approx(3600 + 86400 * 365)
    assert time.monotonic() - real < 1.0


def test_returns_value_and_raises() -> None:
    async def ok() -> int:
        return 42

    async def bad() -> None:
        raise ValueError("boom")

    assert detangle.run(ok) == 42
    with pytest.raises(ValueError, match="boom"):
        detangle.run(bad)


def test_accepts_coroutine_object() -> None:
    async def main(x: int) -> int:
        await asyncio.sleep(1)
        return x * 2

    assert detangle.run(main(21)) == 42


def _program(log: list[str]):
    async def worker(name: str, n: int, q: asyncio.Queue[str], ev: asyncio.Event) -> None:
        for i in range(n):
            log.append(f"{name}{i}")
            await asyncio.sleep(0)
        await q.put(name)
        if name == "b":
            ev.set()
        await ev.wait()
        log.append(f"{name}-done")

    async def main() -> list[str]:
        q: asyncio.Queue[str] = asyncio.Queue()
        ev = asyncio.Event()
        lock = asyncio.Lock()

        async def locked(name: str) -> None:
            async with lock:
                log.append(f"lock-{name}")
                await asyncio.sleep(0)
                log.append(f"unlock-{name}")

        await asyncio.gather(
            worker("a", 3, q, ev),
            worker("b", 2, q, ev),
            locked("x"),
            locked("y"),
            worker("c", 1, q, ev),
        )
        await asyncio.sleep(0.01)
        log.append("timer1")
        results = await asyncio.gather(asyncio.sleep(0.02, "late"), asyncio.sleep(0.01, "early"))
        log.extend(results)
        return [q.get_nowait() for _ in range(q.qsize())]

    return main


def test_fifo_order_matches_real_asyncio() -> None:
    real_log: list[str] = []
    real = asyncio.run(_program(real_log)())
    sim_log: list[str] = []
    sim = detangle.run(_program(sim_log))
    assert sim_log == real_log
    assert sim == real


def test_gather_wait_for_and_timeouts() -> None:
    async def main() -> list[str]:
        out = []
        try:
            await asyncio.wait_for(asyncio.sleep(10), timeout=1)
        except asyncio.TimeoutError:
            out.append(f"timeout@{asyncio.get_running_loop().time():g}")
        done, pending = await asyncio.wait(
            [
                asyncio.ensure_future(asyncio.sleep(1, "x")),
                asyncio.ensure_future(asyncio.sleep(5, "y")),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        out.append(",".join(sorted(t.result() for t in done)))
        for t in pending:
            t.cancel()
        results = await asyncio.gather(
            asyncio.sleep(0, 1), asyncio.sleep(0, 2), return_exceptions=True
        )
        out.append(str(results))
        return out

    assert detangle.run(main) == ["timeout@1", "x", "[1, 2]"]


@pytest.mark.skipif(sys.version_info < (3, 11), reason="asyncio.timeout / TaskGroup need 3.11")
def test_timeout_and_taskgroup() -> None:
    async def main() -> tuple[bool, list[int]]:
        timed_out = False
        try:
            async with asyncio.timeout(2):
                await asyncio.sleep(5)
        except TimeoutError:
            timed_out = True
        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(asyncio.sleep(i, i)) for i in range(3)]
        return timed_out, [t.result() for t in tasks]

    assert detangle.run(main) == (True, [0, 1, 2])


def test_cancellation_semantics() -> None:
    async def main() -> list[str]:
        log = []

        async def victim() -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                log.append("cancelled")
                raise
            finally:
                log.append("finally")

        task = asyncio.create_task(victim())
        await asyncio.sleep(1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        log.append(f"t={asyncio.get_running_loop().time():g}")
        return log

    assert detangle.run(main) == ["cancelled", "finally", "t=1"]


def test_to_thread_and_executor_are_simulated() -> None:
    def blocking(x: int) -> int:
        return x + 1

    async def main() -> tuple[int, int]:
        loop = asyncio.get_running_loop()
        a = await asyncio.to_thread(blocking, 1)
        b = await loop.run_in_executor(None, blocking, 2)
        return a, b

    assert detangle.run(main) == (2, 3)


def test_executor_exception_propagates() -> None:
    def boom() -> None:
        raise KeyError("x")

    async def main() -> None:
        await asyncio.to_thread(boom)

    with pytest.raises(KeyError):
        detangle.run(main)


def test_call_soon_call_later_order() -> None:
    async def main() -> list[int]:
        loop = asyncio.get_running_loop()
        out: list[int] = []
        loop.call_later(0.2, out.append, 3)
        loop.call_later(0.1, out.append, 2)
        loop.call_soon(out.append, 0)
        loop.call_soon(out.append, 1)
        handle = loop.call_later(0.15, out.append, 99)
        handle.cancel()
        loop.call_at(loop.time() + 0.3, out.append, 4)
        await asyncio.sleep(1)
        return out

    assert detangle.run(main) == [0, 1, 2, 3, 4]


def test_contextvars_propagate() -> None:
    var: contextvars.ContextVar[str] = contextvars.ContextVar("var", default="none")

    async def child() -> str:
        await asyncio.sleep(0)
        return var.get()

    async def main() -> list[str]:
        var.set("parent")
        t = asyncio.create_task(child())
        var.set("changed")
        return [await t, var.get()]

    assert detangle.run(main) == ["parent", "changed"]


def test_async_generators_are_finalized() -> None:
    closed = []

    async def agen():
        try:
            for i in range(10):
                yield i
                await asyncio.sleep(0)
        finally:
            closed.append(True)

    async def main() -> list[int]:
        out = []
        async for x in agen():
            out.append(x)
            if x == 2:
                break
        return out

    assert detangle.run(main) == [0, 1, 2]
    assert closed == [True]


def test_current_task_and_all_tasks() -> None:
    async def main() -> tuple[bool, int]:
        me = asyncio.current_task()
        t = asyncio.create_task(asyncio.sleep(1))
        n = len(asyncio.all_tasks())
        await t
        return me is not None and me.get_loop() is asyncio.get_running_loop(), n

    assert detangle.run(main) == (True, 2)


def test_leftover_tasks_are_cancelled_like_asyncio_run() -> None:
    cancelled = []

    async def forever() -> None:
        try:
            await asyncio.sleep(1e9)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    async def main() -> str:
        asyncio.create_task(forever())
        await asyncio.sleep(0)
        return "done"

    assert detangle.run(main) == "done"
    assert cancelled == [True]


def test_real_io_is_refused_with_clear_error() -> None:
    import socket

    async def main() -> None:
        loop = asyncio.get_running_loop()
        s = socket.socket()
        try:
            await loop.sock_connect(s, ("example.com", 80))
        finally:
            s.close()

    with pytest.raises(detangle.RealIOError, match="real I/O"):
        detangle.run(main)


def test_run_refuses_nested_loop() -> None:
    async def outer() -> None:
        async def inner() -> None:
            pass

        detangle.run(inner)

    with pytest.raises(RuntimeError, match="another event loop"):
        asyncio.run(outer())


def test_global_random_is_seeded_and_restored() -> None:
    import random

    async def main() -> float:
        return random.random()

    random.seed(123)
    before = random.getstate()
    a = detangle.run(main)
    assert random.getstate() == before
    b = detangle.run(main)
    assert a == b  # default schedule -> same seed


def test_patch_time() -> None:
    async def main() -> float:
        start = time.monotonic()
        await asyncio.sleep(120)
        return time.monotonic() - start

    assert detangle.run(main, patch_time=True) == pytest.approx(120)
    assert time.monotonic() > 0  # restored


def test_step_cost_advances_time() -> None:
    async def main() -> float:
        for _ in range(10):
            await asyncio.sleep(0)
        return asyncio.get_running_loop().time()

    assert detangle.run(main, step_cost=0.001) == pytest.approx(0.011, abs=1e-9)


def test_run_forever_and_stop() -> None:
    loop = SimLoop()
    out = []
    loop.call_later(5, out.append, "tick")
    loop.call_later(6, loop.stop)
    loop.run_forever()
    assert out == ["tick"]
    assert loop.time() == 6
    loop.close()
    assert loop.is_closed()


def test_run_forever_with_nothing_to_do_is_a_deadlock() -> None:
    loop = SimLoop()
    with pytest.raises(detangle.DeadlockError):
        loop.run_forever()
    loop.close()


def test_exception_handler_is_respected() -> None:
    seen = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, ctx: seen.append(ctx["message"]))
        loop.call_soon(lambda: 1 / 0)
        await asyncio.sleep(0.1)

    detangle.run(main)
    assert seen and "Exception in callback" in seen[0]


def test_eager_task_factory() -> None:
    if not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("eager tasks need Python 3.12+")

    async def main() -> list[str]:
        log: list[str] = []
        asyncio.get_running_loop().set_task_factory(asyncio.eager_task_factory)

        async def child() -> None:
            log.append("child-start")
            await asyncio.sleep(0)
            log.append("child-end")

        t = asyncio.create_task(child())
        log.append("after-create")
        await t
        return log

    assert detangle.run(main) == ["child-start", "after-create", "child-end"]
