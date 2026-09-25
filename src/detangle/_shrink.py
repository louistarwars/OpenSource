"""Schedule shrinking.

A failing run is a list of integers (its decisions).  Because *any* list of
integers is a valid schedule (out-of-range values are clamped, missing values
default to 0), we can shrink failing schedules exactly like Hypothesis
shrinks test inputs: try simpler lists and keep them if they still fail the
same way.

"Simpler" means, in order: fewer non-default decisions (fewer deviations from
asyncio's normal behaviour), smaller values, shorter.  The result is usually
a schedule with one or two deviations -- the essence of the race.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Hashable
from dataclasses import dataclass

from ._choices import complexity, strip_trailing_zeros
from ._runner import RunResult

__all__ = ["ShrinkResult", "shrink"]


@dataclass
class ShrinkResult:
    values: list[int]
    result: RunResult | None
    attempts: int
    improvements: int


def shrink(
    run: Callable[[list[int]], RunResult],
    values: list[int],
    signature: Hashable,
    *,
    budget: int = 400,
    time_limit: float = 30.0,
) -> ShrinkResult:
    best = strip_trailing_zeros(values)
    best_result: RunResult | None = None
    tried: set[tuple[int, ...]] = {tuple(best)}
    attempts = 0
    improvements = 0
    deadline = time.monotonic() + time_limit

    def attempt(candidate: list[int]) -> bool:
        nonlocal best, best_result, attempts, improvements
        candidate = strip_trailing_zeros(candidate)
        key = tuple(candidate)
        if key in tried or complexity(candidate) >= complexity(best):
            return False
        if attempts >= budget or time.monotonic() > deadline:
            return False
        tried.add(key)
        attempts += 1
        result = run(candidate)
        if result.failure is None or result.failure.signature() != signature:
            return False
        actual = strip_trailing_zeros(result.values)
        if complexity(actual) >= complexity(best):
            # The candidate itself was simpler; the clamped actual run was not.
            actual = candidate
        best, best_result = actual, result
        tried.add(tuple(actual))
        improvements += 1
        return True

    def exhausted() -> bool:
        return attempts >= budget or time.monotonic() > deadline

    changed = True
    while changed and not exhausted():
        changed = False

        # 1. Drop suffixes: decisions after the failure point are irrelevant,
        #    and a shorter prefix means "asyncio's default from there on".
        step = max(1, len(best) // 2)
        while step >= 1 and best and not exhausted():
            if attempt(best[: len(best) - step]):
                changed = True
            else:
                step //= 2

        # 2. Reset chunks of decisions to their default (0).
        size = max(1, len(best) // 2)
        while size >= 1 and not exhausted():
            i = 0
            while i < len(best) and not exhausted():
                chunk = best[i : i + size]
                if any(chunk) and attempt(best[:i] + [0] * len(chunk) + best[i + size :]):
                    changed = True
                else:
                    i += size
            size //= 2

        # 3. Lower individual values.
        i = 0
        while i < len(best) and not exhausted():
            value = best[i]
            if value > 1:
                for lower in sorted({1, value // 2, value - 1}):
                    if lower < value and attempt([*best[:i], lower, *best[i + 1 :]]):
                        changed = True
                        break
            i += 1

        # 4. Delete decisions (shifts later ones earlier; realigns schedules).
        for size in (1, 2, 4):
            i = 0
            while i + size <= len(best) and not exhausted():
                if attempt(best[:i] + best[i + size :]):
                    changed = True
                else:
                    i += 1

    return ShrinkResult(best, best_result, attempts, improvements)
