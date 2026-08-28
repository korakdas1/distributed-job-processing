"""Allowlisted task handlers. Only types already validated by the API may run."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from job_platform.core.enums import JobType
from job_platform.tasks.context import NullTaskContext, TaskExecutionContext
from job_platform.tasks.errors import NonRetryableTaskError, RetryableTaskError

Handler = Callable[
    [Mapping[str, Any], TaskExecutionContext],
    Awaitable[dict[str, Any]],
]


class SimulatedTaskFailure(NonRetryableTaskError):
    """Default simulate_failure mode. Terminal non-retryable failure."""

    def __init__(self, message: str = "The simulate_failure task failed intentionally.") -> None:
        super().__init__("SIMULATED_FAILURE", message)


class UnknownHandlerError(Exception):
    """No handler is registered for the job type."""


def _context(context: TaskExecutionContext | None) -> TaskExecutionContext:
    return context if context is not None else NullTaskContext()


async def handle_word_count(
    payload: Mapping[str, Any],
    context: TaskExecutionContext | None = None,
) -> dict[str, Any]:
    ctx = _context(context)
    await ctx.checkpoint()
    text = str(payload["text"])
    result = {"word_count": len(text.split())}
    await ctx.checkpoint()
    return result


async def handle_sum_numbers(
    payload: Mapping[str, Any],
    context: TaskExecutionContext | None = None,
) -> dict[str, Any]:
    ctx = _context(context)
    await ctx.checkpoint()
    numbers = payload["numbers"]
    total = sum(float(item) for item in numbers)
    await ctx.checkpoint()
    return {"sum": total}


async def handle_sleep(
    payload: Mapping[str, Any],
    context: TaskExecutionContext | None = None,
) -> dict[str, Any]:
    ctx = _context(context)
    seconds = float(payload["seconds"])
    remaining = seconds
    chunk = ctx.sleep_chunk_seconds
    while remaining > 0:
        await ctx.checkpoint()
        step = min(chunk, remaining)
        await asyncio.sleep(step)
        remaining -= step
    await ctx.checkpoint()
    return {"slept_seconds": seconds}


async def handle_prime_calculation(
    payload: Mapping[str, Any],
    context: TaskExecutionContext | None = None,
) -> dict[str, Any]:
    ctx = _context(context)
    await ctx.checkpoint()
    limit = int(payload["limit"])
    sieve = [False, False] + [True] * (limit - 1)
    candidate = 2
    last_yield = time.monotonic()
    while candidate * candidate <= limit:
        if sieve[candidate]:
            for multiple in range(candidate * candidate, limit + 1, candidate):
                sieve[multiple] = False
        candidate += 1
        if candidate % 64 == 0:
            now = time.monotonic()
            if now - last_yield >= 0.005:
                await asyncio.sleep(0)
                await ctx.checkpoint()
                last_yield = now
    primes = [index for index, is_prime in enumerate(sieve) if is_prime]
    await ctx.checkpoint()
    return {"count": len(primes), "largest_prime": primes[-1]}


async def handle_simulate_failure(
    payload: Mapping[str, Any],
    context: TaskExecutionContext | None = None,
) -> dict[str, Any]:
    ctx = _context(context)
    await ctx.checkpoint()
    mode = str(payload.get("mode") or "non_retryable")
    if mode == "retryable":
        raise RetryableTaskError(
            "SIMULATED_RETRYABLE_FAILURE",
            "The simulate_failure task failed retryably.",
        )
    raise SimulatedTaskFailure()


HANDLERS: dict[JobType, Handler] = {
    JobType.WORD_COUNT: handle_word_count,
    JobType.SUM_NUMBERS: handle_sum_numbers,
    JobType.SLEEP: handle_sleep,
    JobType.PRIME_CALCULATION: handle_prime_calculation,
    JobType.SIMULATE_FAILURE: handle_simulate_failure,
}


async def execute_handler(
    job_type: JobType,
    payload: Mapping[str, Any],
    context: TaskExecutionContext | None = None,
) -> dict[str, Any]:
    try:
        handler = HANDLERS[job_type]
    except KeyError as exc:
        raise UnknownHandlerError(f"No handler registered for {job_type}") from exc
    return await handler(payload, _context(context))
