import pytest

from job_platform.core.config import Settings
from job_platform.core.enums import JobPriority
from job_platform.core.errors import AppError
from job_platform.idempotency.hashing import (
    canonical_request_fingerprint,
    hash_idempotency_key,
    validate_idempotency_key,
)
from job_platform.schemas.job import JobCreateRequest
from job_platform.tasks.errors import TaskCancelledError, TaskExecutionError


def _settings() -> Settings:
    return Settings(
        postgres_password="unused",
        retry_max_attempts=5,
        job_lease_timeout_seconds=60,
    )


def test_hash_is_deterministic_and_distinct() -> None:
    first = hash_idempotency_key("alpha")
    second = hash_idempotency_key("alpha")
    other = hash_idempotency_key("beta")
    assert first == second
    assert first != other
    assert len(first) == 64
    assert first != "alpha"


def test_fingerprint_ignores_json_key_order() -> None:
    settings = _settings()
    left = JobCreateRequest.model_validate(
        {"job_type": "word_count", "payload": {"text": "hello"}, "priority": "NORMAL"}
    )
    right = JobCreateRequest.model_validate(
        {"priority": "NORMAL", "payload": {"text": "hello"}, "job_type": "word_count"}
    )
    assert canonical_request_fingerprint(left, settings) == canonical_request_fingerprint(
        right, settings
    )


def test_fingerprint_normalizes_priority_default() -> None:
    settings = _settings()
    omitted = JobCreateRequest(job_type="word_count", payload={"text": "hello"})
    explicit = JobCreateRequest(
        job_type="word_count",
        payload={"text": "hello"},
        priority=JobPriority.NORMAL,
    )
    assert canonical_request_fingerprint(omitted, settings) == canonical_request_fingerprint(
        explicit, settings
    )


def test_fingerprint_normalizes_max_attempts_default() -> None:
    settings = _settings()
    omitted = JobCreateRequest(job_type="word_count", payload={"text": "hello"})
    explicit = JobCreateRequest(job_type="word_count", payload={"text": "hello"}, max_attempts=5)
    assert canonical_request_fingerprint(omitted, settings) == canonical_request_fingerprint(
        explicit, settings
    )


def test_fingerprint_changes_with_payload_priority_attempts_delay() -> None:
    settings = _settings()
    base = JobCreateRequest(job_type="word_count", payload={"text": "hello"})
    payload = JobCreateRequest(job_type="word_count", payload={"text": "other"})
    priority = JobCreateRequest(
        job_type="word_count", payload={"text": "hello"}, priority=JobPriority.HIGH
    )
    attempts = JobCreateRequest(job_type="word_count", payload={"text": "hello"}, max_attempts=3)
    delay = JobCreateRequest(job_type="word_count", payload={"text": "hello"}, delay_seconds=2)
    base_fp = canonical_request_fingerprint(base, settings)
    assert canonical_request_fingerprint(payload, settings) != base_fp
    assert canonical_request_fingerprint(priority, settings) != base_fp
    assert canonical_request_fingerprint(attempts, settings) != base_fp
    assert canonical_request_fingerprint(delay, settings) != base_fp


def test_invalid_idempotency_key_rejected() -> None:
    with pytest.raises(AppError) as empty:
        validate_idempotency_key("")
    assert empty.value.code == "INVALID_IDEMPOTENCY_KEY"
    with pytest.raises(AppError):
        validate_idempotency_key("a" * 129)
    with pytest.raises(AppError):
        validate_idempotency_key("bad\nkey")


def test_task_cancelled_is_not_task_execution_error() -> None:
    assert not issubclass(TaskCancelledError, TaskExecutionError)
