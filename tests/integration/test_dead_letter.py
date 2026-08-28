"""Dead-letter publication and operational jobs:dead stream."""

from __future__ import annotations

import uuid

import psycopg
import pytest
from httpx import AsyncClient
from redis.exceptions import RedisError, ResponseError
from sqlalchemy.exc import SQLAlchemyError

from job_platform.core.config import get_settings
from job_platform.models.outbox import OUTBOX_EVENT_JOB_DEAD_LETTER
from job_platform.queue.client import get_redis
from job_platform.queue.dead_letter import publish_dead_letter
from tests.integration.helpers import (
    consumer_pending,
    dead_letter_entries,
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


async def test_non_retryable_failure_skips_retry(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "simulate_failure", "payload": {}, "max_attempts": 5},
    )
    job_id = created.json()["id"]
    await process_next_message()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "FAILED"
    assert body["attempt_count"] == 1
    assert body["next_retry_at"] is None
    assert body["error"]["code"] == "SIMULATED_FAILURE"
    assert body["error"]["retryable"] is False
    assert await delayed_members() == []
    with _pg() as conn:
        events = conn.execute(
            "SELECT event_type FROM outbox_events WHERE job_id = %s",
            (job_id,),
        ).fetchall()
        attempts = conn.execute(
            "SELECT COUNT(*) FROM job_attempts WHERE job_id = %s",
            (job_id,),
        ).fetchone()
    assert attempts is not None and attempts[0] == 1
    assert any(row[0] == OUTBOX_EVENT_JOB_DEAD_LETTER for row in events)


async def test_dead_letter_stream_after_publish(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "simulate_failure", "payload": {}},
    )
    job_id = created.json()["id"]
    await process_next_message()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "FAILED"
    await drain_outbox()
    entries = await dead_letter_entries()
    assert len(entries) == 1
    _mid, fields = entries[0]
    assert fields["job_id"] == job_id
    assert "outbox_event_id" in fields
    assert fields["reason"] == "NON_RETRYABLE_FAILURE"
    assert fields["attempt_count"] == "1"
    assert fields.get("error_code") == "SIMULATED_FAILURE"
    with _pg() as conn:
        event = conn.execute(
            """
            SELECT published_at, redis_message_id FROM outbox_events
            WHERE job_id = %s AND event_type = %s
            """,
            (job_id, OUTBOX_EVENT_JOB_DEAD_LETTER),
        ).fetchone()
    assert event is not None
    assert event[0] is not None
    assert event[1] == _mid


async def test_dead_letter_is_not_consumed_by_workers(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "simulate_failure", "payload": {}},
    )
    job_id = created.json()["id"]
    await process_next_message()
    await drain_outbox()
    assert await dead_letter_entries()
    await process_next_message()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "FAILED"
    assert body["attempt_count"] == 1
    settings = get_settings()
    try:
        groups = await get_redis().xinfo_groups(settings.redis_dead_letter_stream)
    except ResponseError:
        groups = []
    assert groups == []


async def test_dead_letter_redis_outage_replays(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "simulate_failure", "payload": {}},
    )
    job_id = created.json()["id"]
    await process_next_message()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "FAILED"

    async def fail_dlq(*_args: object, **_kwargs: object) -> str:
        raise RedisError("injected dlq outage")

    monkeypatch.setattr("job_platform.outbox.publisher.publish_dead_letter", fail_dlq)
    with pytest.raises(RedisError):
        await drain_outbox()
    with _pg() as conn:
        event = conn.execute(
            """
            SELECT published_at, last_error FROM outbox_events
            WHERE job_id = %s AND event_type = %s
            """,
            (job_id, OUTBOX_EVENT_JOB_DEAD_LETTER),
        ).fetchone()
        status = conn.execute("SELECT status FROM jobs WHERE id = %s", (job_id,)).fetchone()
    assert status is not None and status[0] == "FAILED"
    assert event is not None
    assert event[0] is None
    assert event[1] is not None
    assert await dead_letter_entries() == []

    monkeypatch.setattr(
        "job_platform.outbox.publisher.publish_dead_letter",
        publish_dead_letter,
    )
    assert await drain_outbox() == 1
    entries = await dead_letter_entries()
    assert len(entries) == 1
    assert entries[0][1]["job_id"] == job_id
    still = (await client.get(f"/jobs/{job_id}")).json()
    assert still["status"] == "FAILED"
    assert still["attempt_count"] == 1


async def test_dead_letter_duplicate_publish_window(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "simulate_failure", "payload": {}},
    )
    job_id = created.json()["id"]
    await process_next_message()
    real = publish_dead_letter

    async def crash_after_xadd(
        published_job_id: uuid.UUID,
        *,
        outbox_event_id: uuid.UUID,
        reason: str,
        attempt_count: int,
        error_code: str | None = None,
    ) -> str:
        await real(
            published_job_id,
            outbox_event_id=outbox_event_id,
            reason=reason,
            attempt_count=attempt_count,
            error_code=error_code,
        )
        raise RuntimeError("injected crash after DLQ XADD")

    monkeypatch.setattr("job_platform.outbox.publisher.publish_dead_letter", crash_after_xadd)
    with pytest.raises(RuntimeError, match="injected crash after DLQ XADD"):
        await drain_outbox()
    assert len(await dead_letter_entries()) == 1
    monkeypatch.setattr("job_platform.outbox.publisher.publish_dead_letter", real)
    assert await drain_outbox() == 1
    assert len(await dead_letter_entries()) >= 2
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "FAILED"
    assert body["attempt_count"] == 1
    await process_next_message()
    assert (await client.get(f"/jobs/{job_id}")).json()["attempt_count"] == 1


async def test_dead_letter_commit_failure_does_not_ack(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "simulate_failure", "payload": {}},
    )
    job_id = created.json()["id"]

    async def boom(*_args: object, **_kwargs: object) -> None:
        raise SQLAlchemyError("injected dead-letter persist failure")

    monkeypatch.setattr("job_platform.worker.processor.persist_handler_failure", boom)
    await process_next_message()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "RUNNING"
    assert await consumer_pending() == 1


async def test_unknown_outbox_event_type_is_not_published(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "poison event"}},
    )
    job_id = created.json()["id"]
    await drain_outbox()
    with _pg() as conn:
        conn.execute(
            """
            INSERT INTO outbox_events (
                id, job_id, event_type, payload, created_at, publish_attempts
            ) VALUES (
                %s, %s, 'NOT_A_REAL_EVENT', '{}'::jsonb, now(), 0
            )
            """,
            (str(uuid.uuid4()), job_id),
        )
        conn.commit()
    from job_platform.outbox.publisher import UnknownOutboxEventTypeError, publish_available

    with pytest.raises(UnknownOutboxEventTypeError):
        await publish_available(limit=1)
    with _pg() as conn:
        poison = conn.execute(
            """
            SELECT published_at FROM outbox_events
            WHERE job_id = %s AND event_type = 'NOT_A_REAL_EVENT'
            """,
            (job_id,),
        ).fetchone()
    assert poison is not None
    assert poison[0] is None
