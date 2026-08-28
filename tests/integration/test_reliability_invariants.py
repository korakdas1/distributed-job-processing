"""Host-level races and crash-window invariants."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime

import psycopg
import pytest
from httpx import AsyncClient
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from job_platform.core.config import get_settings
from job_platform.db.session import get_session_factory
from job_platform.models.outbox import OUTBOX_EVENT_JOB_DISPATCH, OUTBOX_EVENT_JOB_INITIAL_SCHEDULE
from job_platform.queue.delayed import schedule_delayed
from job_platform.queue.streams import StreamMessage
from job_platform.scheduler.promoter import promote_due_jobs
from job_platform.worker.processor import (
    _lock_job,
    claim_queued_job,
    persist_terminal_outcome,
    process_message,
)
from tests.integration.helpers import (
    TEST_WORKER_ID,
    consumer_pending,
    delayed_members,
    drain_outbox,
    process_next_message,
)
from tests.integration.test_idempotency import WORD_COUNT

pytestmark = pytest.mark.integration


def _pg() -> psycopg.Connection:
    settings = get_settings()
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
    )


def _event_types(job_id: str) -> list[str]:
    with _pg() as conn:
        rows = conn.execute(
            "SELECT event_type FROM outbox_events WHERE job_id = %s ORDER BY created_at",
            (job_id,),
        ).fetchall()
    return [str(row[0]) for row in rows]


def _attempt_count(job_id: str) -> int:
    with _pg() as conn:
        row = conn.execute(
            "SELECT attempt_count, max_attempts FROM jobs WHERE id = %s", (job_id,)
        ).fetchone()
    assert row is not None
    assert int(row[0]) <= int(row[1])
    return int(row[0])


async def test_twenty_five_concurrent_same_key_one_job(client: AsyncClient) -> None:
    for iteration in range(5):
        headers = {"Idempotency-Key": f"idem-same-{iteration}"}
        body = {"job_type": "word_count", "payload": {"text": f"iter-{iteration}"}}
        responses = await asyncio.gather(
            *[client.post("/jobs", json=body, headers=headers) for _ in range(25)]
        )
        assert all(item.status_code == 202 for item in responses)
        ids = {item.json()["id"] for item in responses}
        assert len(ids) == 1
        job_id = next(iter(ids))
        with _pg() as conn:
            jobs = conn.execute("SELECT COUNT(*) FROM jobs WHERE id = %s", (job_id,)).fetchone()
            events = conn.execute(
                "SELECT COUNT(*) FROM outbox_events WHERE job_id = %s", (job_id,)
            ).fetchone()
            keys = conn.execute(
                "SELECT COUNT(*) FROM submission_idempotency WHERE job_id = %s",
                (job_id,),
            ).fetchone()
        assert jobs is not None and int(jobs[0]) == 1
        assert events is not None and int(events[0]) == 1
        assert keys is not None and int(keys[0]) == 1
    print("IDEMPOTENCY_CONCURRENT iterations=5 requests=25")


async def test_concurrent_conflict_one_job_one_key(client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "idem-conflict-race"}
    word = {"job_type": "word_count", "payload": {"text": "left"}}
    numbers = {"job_type": "sum_numbers", "payload": {"numbers": [1, 2, 3]}}
    responses = await asyncio.gather(
        *[client.post("/jobs", json=word, headers=headers) for _ in range(12)],
        *[client.post("/jobs", json=numbers, headers=headers) for _ in range(12)],
    )
    accepted = [item for item in responses if item.status_code == 202]
    conflicts = [item for item in responses if item.status_code == 409]
    assert accepted
    assert conflicts
    assert all(item.json()["error"]["code"] == "IDEMPOTENCY_KEY_CONFLICT" for item in conflicts)
    ids = {item.json()["id"] for item in accepted}
    assert len(ids) == 1
    with _pg() as conn:
        jobs = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
        keys = conn.execute("SELECT COUNT(*) FROM submission_idempotency").fetchone()
    assert jobs is not None and int(jobs[0]) == 1
    assert keys is not None and int(keys[0]) == 1
    print(f"IDEMPOTENCY_CONFLICT_RACE job={next(iter(ids))}")


async def test_concurrent_different_keys_are_independent(client: AsyncClient) -> None:
    responses = await asyncio.gather(
        *[
            client.post(
                "/jobs",
                json={"job_type": "word_count", "payload": {"text": f"k{i}"}},
                headers={"Idempotency-Key": f"idem-diff-{i}"},
            )
            for i in range(20)
        ]
    )
    assert all(item.status_code == 202 for item in responses)
    ids = {item.json()["id"] for item in responses}
    assert len(ids) == 20
    print("DIFFERENT_KEYS jobs=20")


async def test_xack_failure_after_commit_does_not_undo_success(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = await client.post("/jobs", json=WORD_COUNT)
    job_id = created.json()["id"]

    async def boom(*_args: object, **_kwargs: object) -> None:
        raise RedisError("injected xack failure")

    monkeypatch.setattr("job_platform.worker.processor.ack_message", boom)
    message = await process_next_message()
    assert message is not None
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "SUCCEEDED"
    assert _attempt_count(job_id) == 1
    assert await consumer_pending() == 1

    monkeypatch.undo()
    await process_message(TEST_WORKER_ID, message)
    assert (await client.get(f"/jobs/{job_id}")).json()["status"] == "SUCCEEDED"
    assert _attempt_count(job_id) == 1
    print(f"ACK_AFTER_COMMIT job={job_id}")


async def test_persist_failure_does_not_ack(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = await client.post("/jobs", json=WORD_COUNT)
    job_id = created.json()["id"]

    async def boom(*_args: object, **_kwargs: object) -> None:
        raise SQLAlchemyError("injected terminal persist failure")

    monkeypatch.setattr("job_platform.worker.processor.persist_terminal_outcome", boom)
    await process_next_message()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "RUNNING"
    assert await consumer_pending() == 1
    print(f"NO_ACK_BEFORE_COMMIT job={job_id}")


async def test_initial_schedule_publisher_crash_window(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "later"},
            "delay_seconds": 30,
        },
    )
    job_id = created.json()["id"]
    real = schedule_delayed

    async def crash_after_zadd(published_job_id: uuid.UUID, run_at: datetime) -> None:
        await real(published_job_id, run_at)
        raise RuntimeError("injected crash after ZADD")

    monkeypatch.setattr("job_platform.outbox.publisher.schedule_delayed", crash_after_zadd)
    with pytest.raises(RuntimeError, match="injected crash after ZADD"):
        await drain_outbox()
    members = await delayed_members()
    assert [member for member, _score in members] == [job_id]
    monkeypatch.setattr("job_platform.outbox.publisher.schedule_delayed", real)
    assert await drain_outbox() == 1
    assert len(await delayed_members()) == 1
    still = (await client.get(f"/jobs/{job_id}")).json()
    assert still["status"] == "SCHEDULED"
    assert still["attempt_count"] == 0
    print(f"INITIAL_SCHEDULE_DUP_WINDOW job={job_id}")


async def test_cancel_vs_scheduled_promotion_no_double_dispatch(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "soon"},
            "delay_seconds": 0.05,
        },
    )
    job_id = created.json()["id"]
    await drain_outbox()
    await asyncio.sleep(0.08)

    async def cancel() -> int:
        return (await client.delete(f"/jobs/{job_id}")).status_code

    deleted_status, _promoted = await asyncio.gather(cancel(), promote_due_jobs())
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] in {"CANCELLED", "QUEUED"}
    assert body["attempt_count"] == 0
    dispatch = _event_types(job_id).count(OUTBOX_EVENT_JOB_DISPATCH)
    if body["status"] == "CANCELLED":
        assert dispatch <= 1
    else:
        assert dispatch == 1
        second = await client.delete(f"/jobs/{job_id}")
        assert second.status_code == 200
        assert (await client.get(f"/jobs/{job_id}")).json()["status"] == "CANCELLED"
    assert deleted_status in {200, 202, 409}
    print(f"CANCEL_VS_SCHEDULE job={job_id} status={body['status']}")


async def test_cancel_vs_retry_promotion_no_extra_attempt(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "simulate_failure",
            "payload": {"mode": "retryable"},
            "max_attempts": 3,
        },
    )
    job_id = created.json()["id"]
    await process_next_message()
    retrying = (await client.get(f"/jobs/{job_id}")).json()
    assert retrying["status"] == "RETRYING"
    await drain_outbox()
    await asyncio.sleep(0.2)

    async def cancel() -> int:
        return (await client.delete(f"/jobs/{job_id}")).status_code

    await asyncio.gather(cancel(), promote_due_jobs())
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] in {"CANCELLED", "QUEUED"}
    assert body["attempt_count"] <= body["max_attempts"]
    assert _attempt_count(job_id) <= 3
    print(f"CANCEL_VS_RETRY job={job_id} status={body['status']}")


async def test_cancel_vs_success_concurrent_consistent(client: AsyncClient) -> None:
    created = await client.post("/jobs", json=WORD_COUNT)
    job_id = uuid.UUID(created.json()["id"])
    await drain_outbox()
    factory = get_session_factory()
    stream = get_settings().redis_stream_normal
    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        attempt = await claim_queued_job(
            session,
            job,
            "worker-race01",
            delivery_stream=stream,
            delivery_message_id="9-0",
        )
        await session.commit()
        attempt_id = attempt.id

    async def succeed() -> None:
        async with factory() as session:
            await persist_terminal_outcome(
                session,
                job_id=job_id,
                attempt_id=attempt_id,
                attempt_number=1,
                worker_id="worker-race01",
                delivery_stream=stream,
                delivery_message_id="9-0",
                success=True,
                result={"word_count": 2},
                error=None,
                duration_ms=1,
            )
            await session.commit()

    async def cancel() -> int:
        return (await client.delete(f"/jobs/{job_id}")).status_code

    _ok, deleted = await asyncio.gather(succeed(), cancel(), return_exceptions=True)
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] in {"SUCCEEDED", "CANCELLED"}
    assert body["attempt_count"] == 1
    if body["status"] == "SUCCEEDED":
        assert deleted in {409, 200, 202} or isinstance(deleted, BaseException)
        later = await client.delete(f"/jobs/{job_id}")
        assert later.status_code == 409
    print(f"CANCEL_VS_SUCCESS job={job_id} status={body['status']}")


async def test_duplicate_ready_delivery_one_attempt(client: AsyncClient) -> None:
    created = await client.post("/jobs", json=WORD_COUNT)
    job_id = uuid.UUID(created.json()["id"])
    await drain_outbox()
    from job_platform.queue.streams import ensure_consumer_groups, publish_job_id, read_one

    await ensure_consumer_groups()
    first = await read_one("worker-dup0001", block_ms=2000)
    assert first is not None
    extra_id = await publish_job_id(job_id, stream=first.stream)
    duplicate = StreamMessage(
        stream=first.stream,
        message_id=extra_id,
        job_id=job_id,
        fields={"job_id": str(job_id)},
    )
    await process_message("worker-dup0001", first)
    await process_message("worker-dup0002", duplicate)
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "SUCCEEDED"
    assert _attempt_count(str(job_id)) == 1
    print(f"DUPLICATE_DELIVERY job={job_id}")


async def test_scheduler_promotion_does_not_create_attempts(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "due"},
            "delay_seconds": 0.05,
        },
    )
    job_id = created.json()["id"]
    await drain_outbox()
    await asyncio.sleep(0.08)
    await promote_due_jobs()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "QUEUED"
    assert body["attempt_count"] == 0
    assert OUTBOX_EVENT_JOB_INITIAL_SCHEDULE in _event_types(job_id)
    print(f"SCHEDULER_NO_ATTEMPT job={job_id}")
