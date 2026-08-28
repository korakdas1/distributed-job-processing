import random

import pytest
from pydantic import ValidationError

from job_platform.core.config import Settings
from job_platform.retry.policy import RetryDecision, compute_retry_delay, decide_retry


def test_exponential_backoff_without_jitter() -> None:
    kwargs = {
        "base_delay_seconds": 2.0,
        "max_delay_seconds": 60.0,
        "jitter_ratio": 0.0,
    }
    assert compute_retry_delay(1, **kwargs) == 2.0
    assert compute_retry_delay(2, **kwargs) == 4.0
    assert compute_retry_delay(3, **kwargs) == 8.0
    assert compute_retry_delay(4, **kwargs) == 16.0


def test_backoff_is_capped() -> None:
    kwargs = {
        "base_delay_seconds": 2.0,
        "max_delay_seconds": 10.0,
        "jitter_ratio": 0.0,
    }
    assert compute_retry_delay(1, **kwargs) == 2.0
    assert compute_retry_delay(3, **kwargs) == 8.0
    assert compute_retry_delay(4, **kwargs) == 10.0
    assert compute_retry_delay(10, **kwargs) == 10.0


def test_zero_jitter_is_exact() -> None:
    assert (
        compute_retry_delay(
            2,
            base_delay_seconds=2.0,
            max_delay_seconds=60.0,
            jitter_ratio=0.0,
        )
        == 4.0
    )


def test_jitter_stays_within_bounds() -> None:
    rng = random.Random(0)
    delay = compute_retry_delay(
        1,
        base_delay_seconds=2.0,
        max_delay_seconds=60.0,
        jitter_ratio=0.10,
        rng=rng,
    )
    assert 2.0 * 0.90 <= delay <= 2.0 * 1.10


def test_jitter_never_exceeds_max_delay() -> None:
    class MaxRng:
        def uniform(self, low: float, high: float) -> float:
            return high

    delay = compute_retry_delay(
        4,
        base_delay_seconds=2.0,
        max_delay_seconds=10.0,
        jitter_ratio=0.50,
        rng=MaxRng(),  # type: ignore[arg-type]
    )
    assert delay == 10.0
    assert delay >= 0.0


def test_retry_decision_matrix() -> None:
    assert decide_retry(retryable=True, attempt_count=1, max_attempts=3) is RetryDecision.RETRY
    assert decide_retry(retryable=True, attempt_count=3, max_attempts=3) is RetryDecision.FAIL
    assert decide_retry(retryable=False, attempt_count=1, max_attempts=5) is RetryDecision.FAIL


def test_retry_config_validation() -> None:
    with pytest.raises(ValidationError):
        Settings(postgres_password="unused", retry_max_attempts=0)
    with pytest.raises(ValidationError):
        Settings(
            postgres_password="unused",
            retry_base_delay_seconds=10,
            retry_max_delay_seconds=1,
        )
    with pytest.raises(ValidationError):
        Settings(postgres_password="unused", retry_jitter_ratio=1.5)
    with pytest.raises(ValidationError):
        Settings(postgres_password="unused", retry_scheduler_batch_size=0)
    settings = Settings(
        postgres_password="unused",
        retry_base_delay_seconds=2,
        retry_max_delay_seconds=2,
        retry_jitter_ratio=0,
    )
    assert settings.retry_max_delay_seconds == 2
