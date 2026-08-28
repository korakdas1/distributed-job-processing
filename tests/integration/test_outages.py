from __future__ import annotations

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from job_platform.api.app import create_app
from job_platform.core.config import get_settings
from job_platform.db.session import dispose_engine
from job_platform.queue.client import dispose_redis
from tests.integration.helpers import stream_entries

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


async def test_redis_down_post_still_accepts_and_ready_stays_ok(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "existing"}},
    )
    existing_id = created.json()["id"]
    original_redis_port = str(get_settings().redis_port)

    monkeypatch.setenv("REDIS_PORT", "1")
    get_settings.cache_clear()
    await dispose_redis()

    health = await client.get("/health")
    assert health.status_code == 200
    listed = await client.get("/jobs")
    assert listed.status_code == 200
    fetched = await client.get(f"/jobs/{existing_id}")
    assert fetched.status_code == 200
    ready = await client.get("/ready")
    assert ready.status_code == 200
    body = ready.json()
    assert body["status"] == "ready"
    assert body["degraded"] is True
    assert body["dependencies"]["postgres"] == "up"
    assert body["dependencies"]["redis"] == "down"
    assert body["checks"]["postgres"] == "ok"

    posted = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "buffered"}},
    )
    assert posted.status_code == 202
    stuck_id = posted.json()["id"]
    stuck = await client.get(f"/jobs/{stuck_id}")
    assert stuck.status_code == 200
    assert stuck.json()["status"] == "QUEUED"
    with _pg() as conn:
        event = conn.execute(
            "SELECT published_at FROM outbox_events WHERE job_id = %s",
            (stuck_id,),
        ).fetchone()
    assert event is not None
    assert event[0] is None

    monkeypatch.setenv("REDIS_PORT", original_redis_port)
    get_settings.cache_clear()
    await dispose_redis()
    job_ids = [fields["job_id"] for _mid, fields in await stream_entries()]
    assert stuck_id not in job_ids


async def test_redis_down_delayed_post_still_accepts(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_redis_port = str(get_settings().redis_port)
    monkeypatch.setenv("REDIS_PORT", "1")
    get_settings.cache_clear()
    await dispose_redis()
    posted = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "delayed buffer"},
            "delay_seconds": 5,
        },
    )
    assert posted.status_code == 202
    body = posted.json()
    assert body["status"] == "SCHEDULED"
    assert body["run_after"] is not None
    assert body["queued_at"] is None
    with _pg() as conn:
        event = conn.execute(
            "SELECT event_type, published_at FROM outbox_events WHERE job_id = %s",
            (body["id"],),
        ).fetchone()
    assert event is not None
    assert event[0] == "JOB_INITIAL_SCHEDULE"
    assert event[1] is None
    ready = await client.get("/ready")
    assert ready.status_code == 200
    monkeypatch.setenv("REDIS_PORT", original_redis_port)
    get_settings.cache_clear()
    await dispose_redis()


async def test_postgres_down_does_not_publish_to_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = await stream_entries()
    monkeypatch.setenv("POSTGRES_PORT", "1")
    get_settings.cache_clear()
    await dispose_engine()
    await dispose_redis()
    application = create_app()
    async with AsyncClient(
        transport=ASGITransport(app=application),
        base_url="http://test",
    ) as isolated:
        health = await isolated.get("/health")
        assert health.status_code == 200
        ready = await isolated.get("/ready")
        assert ready.status_code == 503
        assert ready.json()["checks"]["postgres"] == "error"
        assert ready.json()["status"] == "not_ready"
        assert ready.json()["dependencies"]["postgres"] == "down"
        posted = await isolated.post(
            "/jobs",
            json={"job_type": "word_count", "payload": {"text": "no-db"}},
        )
        assert posted.status_code == 503
        assert posted.json()["error"]["code"] == "DATABASE_UNAVAILABLE"
    await dispose_engine()
    get_settings.cache_clear()
    await dispose_redis()
    assert await stream_entries() == before
