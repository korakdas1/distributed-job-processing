import pytest
from pydantic import ValidationError

from job_platform.api.services import create_job_record
from job_platform.core.config import Settings
from job_platform.core.enums import JobPriority, JobStatus
from job_platform.core.errors import AppError
from job_platform.schemas.job import JobCreateRequest


def _settings() -> Settings:
    return Settings(
        postgres_password="unused",
        retry_max_attempts=5,
        job_lease_timeout_seconds=60,
        payload_max_bytes=32 * 1024,
    )


def test_create_job_defaults() -> None:
    job = create_job_record(
        settings=_settings(),
        job_type="word_count",
        payload={"text": "hello"},
        priority=JobPriority.NORMAL,
        max_attempts=None,
        timeout_seconds=None,
        delay_seconds=0,
    )
    assert job.status == JobStatus.QUEUED.value
    assert job.attempt_count == 0
    assert job.max_attempts == 5
    assert job.timeout_seconds == 60
    assert job.result is None
    assert job.error is None
    assert job.worker_id is None
    assert job.queued_at is not None
    assert job.run_after is None
    assert job.next_retry_at is None
    assert job.idempotency_key is None
    assert job.cancel_requested_at is None
    assert job.cancelled_at is None


def test_delay_seconds_creates_scheduled_job() -> None:
    job = create_job_record(
        settings=_settings(),
        job_type="word_count",
        payload={"text": "hello"},
        priority=JobPriority.HIGH,
        max_attempts=None,
        timeout_seconds=None,
        delay_seconds=10,
    )
    assert job.status == JobStatus.SCHEDULED.value
    assert job.run_after is not None
    assert job.queued_at is None
    assert job.attempt_count == 0
    assert job.worker_id is None
    assert job.next_retry_at is None
    assert job.priority == JobPriority.HIGH.value


def test_delay_seconds_above_max_rejected() -> None:
    with pytest.raises(AppError) as exc_info:
        create_job_record(
            settings=_settings(),
            job_type="word_count",
            payload={"text": "hello"},
            priority=JobPriority.NORMAL,
            max_attempts=None,
            timeout_seconds=None,
            delay_seconds=604801,
        )
    assert exc_info.value.code == "DELAY_TOO_LARGE"


def test_delay_seconds_at_max_accepted() -> None:
    job = create_job_record(
        settings=_settings(),
        job_type="word_count",
        payload={"text": "hello"},
        priority=JobPriority.NORMAL,
        max_attempts=None,
        timeout_seconds=None,
        delay_seconds=604800,
    )
    assert job.status == JobStatus.SCHEDULED.value
    assert job.run_after is not None


def test_invalid_max_attempts_on_request() -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest(job_type="word_count", max_attempts=0)


def test_invalid_timeout_on_request() -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest(job_type="word_count", timeout_seconds=0)


def test_request_defaults() -> None:
    body = JobCreateRequest(job_type="word_count")
    assert body.payload == {}
    assert body.priority is JobPriority.NORMAL
    assert body.max_attempts is None
    assert body.timeout_seconds is None
    assert body.delay_seconds == 0


def test_invalid_priority_on_request() -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate({"job_type": "word_count", "priority": "URGENT"})


def test_non_finite_delay_rejected_on_request() -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest(job_type="word_count", delay_seconds=float("inf"))
    with pytest.raises(ValidationError):
        JobCreateRequest(job_type="word_count", delay_seconds=float("nan"))
