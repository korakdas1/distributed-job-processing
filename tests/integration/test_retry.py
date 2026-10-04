"""Task retry, backoff, scheduler promotion, and attempt-budget tests."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from httpx import AsyncClient
from sqlalchemy.exc import SQLAlchemyError

from job_platform.core.clock import utcnow
from job_platform.core.config import get_settings
from job_platform.db.session import get_session_factory
from job_platform.models.job import JobAttempt
from job_platform.models.outbox import (
    OUTBOX_EVENT_JOB_DEAD_LETTER,
    OUTBOX_EVENT_JOB_DISPATCH,
    OUTBOX_EVENT_JOB_RETRY_SCHEDULE,
)
from job_platform.queue.client import get_redis
from job_platform.queue.delayed import schedule_delayed
from job_platform.queue.streams import StreamMessage, publish_job_id
from job_platform.scheduler.promoter import promote_due_jobs
from job_platform.tasks.errors import RetryableTaskError
from job_platform.tasks.handlers import execute_handler
from job_platform.worker.processor import (
    WorkerOwnershipError,
    _lock_job,
    claim_queued_job,
    persist_handler_failure,
    process_message,
    recover_crashed_attempt,
)
from tests.integration.helpers import (
    consumer_pending,
    delayed_members,
    drain_outbox,
    process_next_message,
)

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


def _events(job_id: str) -> list[tuple[str, object, object]]:
    with _pg() as conn:
        return conn.execute(
            """
            SELECT event_type, published_at, payload
            FROM outbox_events WHERE job_id = %s ORDER BY created_at, id
            """,
            (job_id,),
        ).fetchall()


def _attempts(job_id: str) -> list[tuple[int, str, object]]:
    with _pg() as conn:
        return conn.execute(
            """
            SELECT attempt_number, status, error
            FROM job_attempts WHERE job_id = %s ORDER BY attempt_number
            """,
            (job_id,),
        ).fetchall()


async def _wait_due(next_retry_at: datetime) -> None:
    remaining = (next_retry_at - utcnow()).total_seconds()
    if remaining > 0:
        await asyncio.sleep(remaining + 0.05)


async def test_first_retryable_failure_becomes_retrying(client: AsyncClient) -> None:
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
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "RETRYING"
    assert body["attempt_count"] == 1
    assert body["next_retry_at"] is not None
    assert body["completed_at"] is None
    assert body["worker_id"] is None
    assert body["error"]["code"] == "SIMULATED_RETRYABLE_FAILURE"
    assert body["error"]["retryable"] is True
    listed = await client.get("/jobs", params={"status": "RETRYING"})
    assert listed.json()["total"] == 1
    assert listed.json()["items"][0]["id"] == job_id

    attempts = _attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0][1] == "FAILED"
    assert attempts[0][2]["retryable"] is True
    events = _events(job_id)
    retry_events = [row for row in events if row[0] == OUTBOX_EVENT_JOB_RETRY_SCHEDULE]
    assert len(retry_events) == 1
    assert retry_events[0][1] is None
    payload = retry_events[0][2]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["failed_attempt"] == 1
    assert "run_at" in payload
    assert await consumer_pending() == 0


async def test_no_early_retry_and_delayed_zset(client: AsyncClient) -> None:
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
    body = (await client.get(f"/jobs/{job_id}")).json()
    next_retry_at = datetime.fromisoformat(body["next_retry_at"])
    with _pg() as conn:
        finished_at = conn.execute(
            "SELECT finished_at FROM job_attempts WHERE job_id = %s AND attempt_number = 1",
            (job_id,),
        ).fetchone()
    assert finished_at is not None
    finished = finished_at[0]
    if finished.tzinfo is None:
        finished = finished.replace(tzinfo=UTC)
    delay = (next_retry_at - finished).total_seconds()
    settings = get_settings()
    assert abs(delay - settings.retry_base_delay_seconds) < 0.08
    await drain_outbox()
    members = await delayed_members()
    assert any(member == job_id for member, _score in members)
    score = next(score for member, score in members if member == job_id)
    assert abs(score - next_retry_at.timestamp()) < 0.05
    assert body["attempt_count"] == 1

    await promote_due_jobs()
    still = (await client.get(f"/jobs/{job_id}")).json()
    assert still["status"] == "RETRYING"
    assert still["attempt_count"] == 1
    assert still["next_retry_at"] is not None


async def test_retry_promotion_creates_next_attempt(client: AsyncClient) -> None:
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
    body = (await client.get(f"/jobs/{job_id}")).json()
    await drain_outbox()
    await _wait_due(datetime.fromisoformat(body["next_retry_at"]))
    await promote_due_jobs()
    promoted = (await client.get(f"/jobs/{job_id}")).json()
    assert promoted["status"] == "QUEUED"
    assert promoted["next_retry_at"] is None
    assert promoted["queued_at"] is not None
    assert promoted["attempt_count"] == 1
    assert await delayed_members() == []
    events = _events(job_id)
    dispatch = [row for row in events if row[0] == OUTBOX_EVENT_JOB_DISPATCH]
    assert len(dispatch) == 2
    await process_next_message()
    after = (await client.get(f"/jobs/{job_id}")).json()
    assert after["status"] == "RETRYING"
    assert after["attempt_count"] == 2
    assert len(_attempts(job_id)) == 2


async def test_retryable_exhaustion_dead_letters(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "simulate_failure",
            "payload": {"mode": "retryable"},
            "max_attempts": 3,
        },
    )
    job_id = created.json()["id"]
    delays: list[float] = []
    for expected_attempt in (1, 2, 3):
        await process_next_message()
        body = (await client.get(f"/jobs/{job_id}")).json()
        if expected_attempt < 3:
            assert body["status"] == "RETRYING"
            assert body["attempt_count"] == expected_attempt
            with _pg() as conn:
                finished_at = conn.execute(
                    """
                    SELECT finished_at FROM job_attempts
                    WHERE job_id = %s AND attempt_number = %s
                    """,
                    (job_id, expected_attempt),
                ).fetchone()
            assert finished_at is not None
            next_retry_at = datetime.fromisoformat(body["next_retry_at"])
            finished = finished_at[0]
            if finished.tzinfo is None:
                finished = finished.replace(tzinfo=UTC)
            delay = (next_retry_at - finished).total_seconds()
            delays.append(delay)
            await drain_outbox()
            await _wait_due(next_retry_at)
            await promote_due_jobs()
        else:
            assert body["status"] == "FAILED"
            assert body["attempt_count"] == 3
            assert body["next_retry_at"] is None
            assert body["completed_at"] is not None
            assert body["error"]["code"] == "MAX_ATTEMPTS_EXHAUSTED"
            assert body["error"]["retryable"] is False
            assert body["error"]["last_error_code"] == "SIMULATED_RETRYABLE_FAILURE"

    settings = get_settings()
    assert abs(delays[0] - settings.retry_base_delay_seconds) < 0.08
    assert abs(delays[1] - (2 * settings.retry_base_delay_seconds)) < 0.08
    attempts = _attempts(job_id)
    assert len(attempts) == 3
    assert all(row[1] == "FAILED" for row in attempts)
    assert all(row[2]["code"] == "SIMULATED_RETRYABLE_FAILURE" for row in attempts)
    events = _events(job_id)
    assert any(row[0] == OUTBOX_EVENT_JOB_DEAD_LETTER for row in events)
    assert not any(
        row[0] == OUTBOX_EVENT_JOB_RETRY_SCHEDULE and row[1] is None for row in events[-1:]
    )


async def test_max_attempts_one_does_not_retry(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "simulate_failure",
            "payload": {"mode": "retryable"},
            "max_attempts": 1,
        },
    )
    job_id = created.json()["id"]
    await process_next_message()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "FAILED"
    assert body["attempt_count"] == 1
    assert body["next_retry_at"] is None
    assert body["error"]["code"] == "MAX_ATTEMPTS_EXHAUSTED"
    assert await delayed_members() == []
    assert any(row[0] == OUTBOX_EVENT_JOB_DEAD_LETTER for row in _events(job_id))
    assert not any(row[0] == OUTBOX_EVENT_JOB_RETRY_SCHEDULE for row in _events(job_id))


async def test_success_after_retryable_failure(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}
    real = execute_handler

    async def flaky(job_type: object, payload: object, context: object = None) -> dict[str, object]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RetryableTaskError("SIMULATED_RETRYABLE_FAILURE", "first attempt")
        return await real(job_type, payload, context)  # type: ignore[arg-type]

    monkeypatch.setattr("job_platform.worker.processor.execute_handler", flaky)
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "hello world"}, "max_attempts": 3},
    )
    job_id = created.json()["id"]
    await process_next_message()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "RETRYING"
    await drain_outbox()
    await _wait_due(datetime.fromisoformat(body["next_retry_at"]))
    await promote_due_jobs()
    await process_next_message()
    done = (await client.get(f"/jobs/{job_id}")).json()
    assert done["status"] == "SUCCEEDED"
    assert done["attempt_count"] == 2
    assert done["error"] is None
    assert done["result"] == {"word_count": 2}
    attempts = _attempts(job_id)
    assert [row[1] for row in attempts] == ["FAILED", "SUCCEEDED"]


async def test_retrying_stale_delivery_is_acked(client: AsyncClient) -> None:
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
    before = (await client.get(f"/jobs/{job_id}")).json()
    next_retry_at = before["next_retry_at"]
    await publish_job_id(uuid.UUID(job_id), stream=get_settings().redis_stream_normal)
    await process_next_message("worker-stale001")
    after = (await client.get(f"/jobs/{job_id}")).json()
    assert after["status"] == "RETRYING"
    assert after["attempt_count"] == 1
    assert after["next_retry_at"] == next_retry_at
    assert len(_attempts(job_id)) == 1
    assert await consumer_pending() == 0


async def test_retry_state_commit_failure_does_not_ack(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "simulate_failure",
            "payload": {"mode": "retryable"},
            "max_attempts": 3,
        },
    )
    job_id = created.json()["id"]

    async def boom(*_args: object, **_kwargs: object) -> None:
        raise SQLAlchemyError("injected retry persist failure")

    monkeypatch.setattr("job_platform.worker.processor.persist_handler_failure", boom)
    await process_next_message()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "RUNNING"
    assert await consumer_pending() == 1
    assert not any(row[0] == OUTBOX_EVENT_JOB_RETRY_SCHEDULE for row in _events(job_id))


async def test_stale_worker_cannot_schedule_retry(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "simulate_failure",
            "payload": {"mode": "retryable"},
            "max_attempts": 5,
        },
    )
    job_id = uuid.UUID(created.json()["id"])
    factory = get_session_factory()
    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        old = await claim_queued_job(
            session,
            job,
            "worker-aaaa1111",
            delivery_stream="jobs:normal",
            delivery_message_id="1-0",
        )
        await session.commit()
        old_id = old.id

    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        running = await session.get(JobAttempt, old_id)
        assert running is not None
        await recover_crashed_attempt(
            session,
            job,
            running,
            "worker-bbbb2222",
            delivery_stream="jobs:normal",
            delivery_message_id="1-0",
        )
        await session.commit()

    async with factory() as session:
        with pytest.raises(WorkerOwnershipError):
            await persist_handler_failure(
                session,
                job_id=job_id,
                attempt_id=old_id,
                attempt_number=1,
                worker_id="worker-aaaa1111",
                delivery_stream="jobs:normal",
                delivery_message_id="1-0",
                error={
                    "code": "SIMULATED_RETRYABLE_FAILURE",
                    "message": "stale",
                    "retryable": True,
                },
                duration_ms=1,
            )

    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "RUNNING"
    assert fetched.json()["worker_id"] == "worker-bbbb2222"
    assert fetched.json()["attempt_count"] == 2
    assert not any(row[0] == OUTBOX_EVENT_JOB_RETRY_SCHEDULE for row in _events(str(job_id)))


async def test_crash_recovery_consumes_attempt_budget(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "simulate_failure",
            "payload": {"mode": "retryable"},
            "max_attempts": 2,
        },
    )
    job_id = uuid.UUID(created.json()["id"])
    await drain_outbox()
    from job_platform.queue.streams import ensure_consumer_groups, read_one

    await ensure_consumer_groups()
    message = await read_one("worker-aaaa1111", block_ms=2000)
    assert message is not None
    factory = get_session_factory()
    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        await claim_queued_job(
            session,
            job,
            "worker-aaaa1111",
            delivery_stream="jobs:normal",
            delivery_message_id=message.message_id,
        )
        await session.commit()

    await process_message("worker-bbbb2222", message, reclaimed=True)
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "FAILED"
    assert body["attempt_count"] == 2
    assert body["error"]["code"] == "MAX_ATTEMPTS_EXHAUSTED"
    attempts = _attempts(str(job_id))
    assert [row[1] for row in attempts] == ["INTERRUPTED", "FAILED"]
    assert not any(row[0] == OUTBOX_EVENT_JOB_RETRY_SCHEDULE for row in _events(str(job_id)))


async def test_crash_only_budget_exhaustion(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "crash budget"}, "max_attempts": 2},
    )
    job_id = uuid.UUID(created.json()["id"])
    factory = get_session_factory()
    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        first = await claim_queued_job(
            session,
            job,
            "worker-aaaa1111",
            delivery_stream="jobs:normal",
            delivery_message_id="1-0",
        )
        await session.commit()

    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        running = await session.get(JobAttempt, first.id)
        assert running is not None
        await recover_crashed_attempt(
            session,
            job,
            running,
            "worker-bbbb2222",
            delivery_stream="jobs:normal",
            delivery_message_id="1-0",
        )
        await session.commit()

    stale = StreamMessage(
        stream="jobs:normal", message_id="1-0", job_id=job_id, fields={"job_id": str(job_id)}
    )
    await process_message("worker-cccc3333", stale, reclaimed=True)
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "FAILED"
    assert body["attempt_count"] == 2
    assert body["error"]["code"] == "ATTEMPT_BUDGET_EXHAUSTED_AFTER_INTERRUPT"
    attempts = _attempts(str(job_id))
    assert len(attempts) == 2
    assert attempts[0][1] == "INTERRUPTED"
    assert attempts[1][1] == "INTERRUPTED"
    assert any(row[0] == OUTBOX_EVENT_JOB_DEAD_LETTER for row in _events(str(job_id)))


async def test_retry_schedule_publisher_crash_window(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    real = schedule_delayed

    async def crash_after_zadd(
        published_job_id: uuid.UUID, run_at: datetime, *, outbox_event_id: uuid.UUID
    ) -> None:
        await real(published_job_id, run_at, outbox_event_id=outbox_event_id)
        raise RuntimeError("injected crash after ZADD")

    monkeypatch.setattr("job_platform.outbox.publisher.schedule_delayed", crash_after_zadd)
    with pytest.raises(RuntimeError, match="injected crash after ZADD"):
        await drain_outbox()
    members = await delayed_members()
    assert [member for member, _score in members] == [job_id]
    monkeypatch.setattr("job_platform.outbox.publisher.schedule_delayed", real)
    assert await drain_outbox() == 1
    members_after = await delayed_members()
    assert len(members_after) == 1
    assert members_after[0][0] == job_id


async def test_stale_delayed_score_is_rescored(client: AsyncClient) -> None:
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
    await drain_outbox()
    body = (await client.get(f"/jobs/{job_id}")).json()
    next_retry_at = datetime.fromisoformat(body["next_retry_at"])
    await get_redis().zadd(
        get_settings().redis_delayed_zset, {job_id: (utcnow() - timedelta(seconds=30)).timestamp()}
    )
    await promote_due_jobs()
    still = (await client.get(f"/jobs/{job_id}")).json()
    assert still["status"] == "RETRYING"
    members = await delayed_members()
    score = next(score for member, score in members if member == job_id)
    assert abs(score - next_retry_at.timestamp()) < 0.05
    assert still["attempt_count"] == 1
