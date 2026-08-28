import uuid

from job_platform.queue.streams import parse_job_id
from job_platform.worker.runtime import generate_worker_id


def test_parse_valid_job_id() -> None:
    job_id = uuid.uuid4()
    assert parse_job_id({"job_id": str(job_id)}) == job_id


def test_parse_missing_job_id() -> None:
    assert parse_job_id({}) is None
    assert parse_job_id({"job_id": ""}) is None


def test_parse_malformed_job_id() -> None:
    assert parse_job_id({"job_id": "not-a-uuid"}) is None
    assert parse_job_id({"job_id": "123"}) is None


def test_worker_id_format() -> None:
    first = generate_worker_id()
    second = generate_worker_id()
    assert first.startswith("worker-")
    assert second.startswith("worker-")
    assert first != second
    assert len(first) == len("worker-") + 8
