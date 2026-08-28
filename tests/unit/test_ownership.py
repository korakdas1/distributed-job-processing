import uuid

import pytest

from job_platform.core.clock import utcnow
from job_platform.models.job import Job, JobAttempt
from job_platform.worker.processor import WorkerOwnershipError, assert_terminal_ownership
from job_platform.worker.runtime import generate_worker_id


def _job(*, worker_id: str, attempt_count: int = 1, status: str = "RUNNING") -> Job:
    return Job(
        id=uuid.uuid4(),
        job_type="word_count",
        payload={"text": "x"},
        status=status,
        priority="NORMAL",
        attempt_count=attempt_count,
        max_attempts=5,
        timeout_seconds=60,
        worker_id=worker_id,
    )


def _attempt(
    job: Job, *, worker_id: str, status: str = "RUNNING", delivery_message_id: str = "1-0"
) -> JobAttempt:
    return JobAttempt(
        id=uuid.uuid4(),
        job_id=job.id,
        attempt_number=1,
        worker_id=worker_id,
        started_at=utcnow(),
        status=status,
        delivery_message_id=delivery_message_id,
        delivery_stream="jobs:normal",
    )


def test_worker_ids_are_unique_and_shaped() -> None:
    ids = [generate_worker_id() for _ in range(20)]
    assert len(set(ids)) == 20
    for value in ids:
        assert value.startswith("worker-")
        assert len(value) == 15


def test_correct_ownership_accepted() -> None:
    worker_id = "worker-aaaa1111"
    job = _job(worker_id=worker_id)
    attempt = _attempt(job, worker_id=worker_id)
    assert_terminal_ownership(
        job,
        attempt,
        worker_id=worker_id,
        attempt_id=attempt.id,
        attempt_number=1,
        delivery_stream="jobs:normal",
        delivery_message_id="1-0",
    )


def test_wrong_job_worker_id_rejected() -> None:
    job = _job(worker_id="worker-owner01")
    attempt = _attempt(job, worker_id="worker-owner01")
    with pytest.raises(WorkerOwnershipError, match="job.worker_id"):
        assert_terminal_ownership(
            job,
            attempt,
            worker_id="worker-stale02",
            attempt_id=attempt.id,
            attempt_number=1,
            delivery_stream="jobs:normal",
            delivery_message_id="1-0",
        )


def test_wrong_attempt_worker_id_rejected() -> None:
    worker_id = "worker-owner01"
    job = _job(worker_id=worker_id)
    attempt = _attempt(job, worker_id="worker-other02")
    with pytest.raises(WorkerOwnershipError, match="attempt.worker_id"):
        assert_terminal_ownership(
            job,
            attempt,
            worker_id=worker_id,
            attempt_id=attempt.id,
            attempt_number=1,
            delivery_stream="jobs:normal",
            delivery_message_id="1-0",
        )


def test_wrong_attempt_number_rejected() -> None:
    worker_id = "worker-owner01"
    job = _job(worker_id=worker_id, attempt_count=2)
    attempt = _attempt(job, worker_id=worker_id)
    with pytest.raises(WorkerOwnershipError, match="job.attempt_count"):
        assert_terminal_ownership(
            job,
            attempt,
            worker_id=worker_id,
            attempt_id=attempt.id,
            attempt_number=1,
            delivery_stream="jobs:normal",
            delivery_message_id="1-0",
        )


def test_wrong_delivery_message_id_rejected() -> None:
    worker_id = "worker-owner01"
    job = _job(worker_id=worker_id)
    attempt = _attempt(job, worker_id=worker_id, delivery_message_id="1-0")
    with pytest.raises(WorkerOwnershipError, match="delivery_message_id"):
        assert_terminal_ownership(
            job,
            attempt,
            worker_id=worker_id,
            attempt_id=attempt.id,
            attempt_number=1,
            delivery_stream="jobs:normal",
            delivery_message_id="9-9",
        )


def test_non_running_job_rejected() -> None:
    worker_id = "worker-owner01"
    job = _job(worker_id=worker_id, status="SUCCEEDED")
    attempt = _attempt(job, worker_id=worker_id, status="SUCCEEDED")
    with pytest.raises(WorkerOwnershipError):
        assert_terminal_ownership(
            job,
            attempt,
            worker_id=worker_id,
            attempt_id=attempt.id,
            attempt_number=1,
            delivery_stream="jobs:normal",
            delivery_message_id="1-0",
        )


def test_same_message_id_on_different_streams_are_distinct() -> None:
    worker_id = "worker-owner01"
    job = _job(worker_id=worker_id)
    attempt = JobAttempt(
        id=uuid.uuid4(),
        job_id=job.id,
        attempt_number=1,
        worker_id=worker_id,
        started_at=utcnow(),
        status="RUNNING",
        delivery_message_id="123-0",
        delivery_stream="jobs:high",
    )
    assert_terminal_ownership(
        job,
        attempt,
        worker_id=worker_id,
        attempt_id=attempt.id,
        attempt_number=1,
        delivery_stream="jobs:high",
        delivery_message_id="123-0",
    )
    with pytest.raises(WorkerOwnershipError, match="delivery_stream"):
        assert_terminal_ownership(
            job,
            attempt,
            worker_id=worker_id,
            attempt_id=attempt.id,
            attempt_number=1,
            delivery_stream="jobs:low",
            delivery_message_id="123-0",
        )
