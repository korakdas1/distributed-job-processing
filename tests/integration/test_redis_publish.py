from __future__ import annotations

import psycopg
import pytest
from httpx import AsyncClient

from job_platform.core.config import get_settings
from tests.integration.helpers import drain_outbox, stream_entries

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


async def test_post_does_not_call_redis_until_publisher(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "hello distributed world"},
            "priority": "HIGH",
        },
    )
    assert created.status_code == 202
    body = created.json()
    job_id = body["id"]
    assert body["status"] == "QUEUED"
    assert body["priority"] == "HIGH"
    assert await stream_entries() == []
    assert await stream_entries(get_settings().redis_stream_high) == []

    with _pg() as conn:
        job_row = conn.execute(
            "SELECT status, payload FROM jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
        event = conn.execute(
            """
            SELECT event_type, published_at, redis_message_id, publish_attempts
            FROM outbox_events WHERE job_id = %s
            """,
            (job_id,),
        ).fetchone()
    assert job_row is not None
    assert job_row[0] == "QUEUED"
    assert job_row[1]["text"] == "hello distributed world"
    assert event is not None
    assert event[0] == "JOB_DISPATCH"
    assert event[1] is None
    assert event[2] is None
    assert event[3] == 0

    await drain_outbox()
    entries = await stream_entries(get_settings().redis_stream_high)
    assert len(entries) == 1
    _message_id, fields = entries[0]
    assert fields["job_id"] == job_id
    assert "payload" not in fields
    assert "text" not in fields
    assert "job_type" not in fields
    assert await stream_entries() == []


async def test_high_priority_uses_jobs_high(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "sum_numbers",
            "payload": {"numbers": [1, 2]},
            "priority": "CRITICAL",
        },
    )
    assert created.status_code == 202
    await drain_outbox()
    entries = await stream_entries(get_settings().redis_stream_critical)
    assert len(entries) == 1
    assert entries[0][1]["job_id"] == created.json()["id"]
    assert await stream_entries(get_settings().redis_stream_normal) == []
    assert get_settings().redis_stream_normal == "jobs:normal"
