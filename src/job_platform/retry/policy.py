"""Pure retry policy: exponential backoff, jitter, and attempt-budget decisions."""

from __future__ import annotations

import random
from enum import StrEnum


class RetryDecision(StrEnum):
    RETRY = "RETRY"
    FAIL = "FAIL"


def compute_retry_delay(
    attempt_number: int,
    *,
    base_delay_seconds: float,
    max_delay_seconds: float,
    jitter_ratio: float,
    rng: random.Random | None = None,
) -> float:
    """Delay after the given failed execution attempt.

    raw_delay = min(max_delay, base * 2^(attempt_number - 1))
    With jitter, multiply by a value in [1 - ratio, 1 + ratio], then cap again.
    """
    if attempt_number < 1:
        msg = "attempt_number must be >= 1"
        raise ValueError(msg)
    if base_delay_seconds <= 0 or max_delay_seconds <= 0:
        msg = "retry delays must be positive"
        raise ValueError(msg)
    if jitter_ratio < 0 or jitter_ratio > 1:
        msg = "jitter_ratio must be in [0, 1]"
        raise ValueError(msg)

    raw = min(max_delay_seconds, base_delay_seconds * (2 ** (attempt_number - 1)))
    if jitter_ratio == 0:
        return float(raw)
    generator = rng if rng is not None else random.Random()
    multiplier = generator.uniform(1.0 - jitter_ratio, 1.0 + jitter_ratio)
    delay = min(max_delay_seconds, raw * multiplier)
    return float(max(0.0, delay))


def decide_retry(*, retryable: bool, attempt_count: int, max_attempts: int) -> RetryDecision:
    """RETRY only when the failure is retryable and total execution budget remains."""
    if not retryable:
        return RetryDecision.FAIL
    if attempt_count >= max_attempts:
        return RetryDecision.FAIL
    return RetryDecision.RETRY
