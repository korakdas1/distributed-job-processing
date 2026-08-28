"""GET /workers derivation without depending on a live worker process."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest
import redis as redis_sync
from httpx import AsyncClient

from job_platform.core.config import get_settings
from job_platform.worker.heartbeat import heartbeat_key
from tests.integration.process_harness import start_project_redis, stop_project_redis

pytestmark = pytest.mark.integration


def _insert_worker(
    *,
    worker_id: str,
    stopped: bool = False,
    pid: int = 4242,
) -> None:
    settings = get_settings()
    now = datetime.now(UTC)
    stopped_at = now if stopped else None
    with psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
    ) as conn:
        conn.execute(
            """
            INSERT INTO workers (id, started_at, last_seen_at, stopped_at, hostname, pid)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (worker_id, now - timedelta(seconds=5), now, stopped_at, "testhost", pid),
        )
        conn.commit()


async def test_workers_empty_list(client: AsyncClient) -> None:
    response = await client.get("/workers")
    assert response.status_code == 200
    body = response.json()
    assert body["items"] == []
    assert body["total"] == 0
    assert body["liveness_available"] is True


async def test_workers_expired_history_row(client: AsyncClient) -> None:
    _insert_worker(worker_id="worker-expired1")
    response = await client.get("/workers")
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["id"] == "worker-expired1"
    assert item["status"] == "EXPIRED"
    assert item["is_alive"] is False
    assert item["stopped_at"] is None


async def test_workers_stopped_history_row(client: AsyncClient) -> None:
    _insert_worker(worker_id="worker-stopped1", stopped=True)
    response = await client.get("/workers")
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["status"] == "STOPPED"
    assert item["is_alive"] is False
    assert item["stopped_at"] is not None


async def test_stopped_wins_over_stale_heartbeat(client: AsyncClient) -> None:
    _insert_worker(worker_id="worker-stopped2", stopped=True)
    settings = get_settings()
    redis = redis_sync.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
    )
    try:
        redis.set(
            heartbeat_key("worker-stopped2"),
            '{"worker_id":"worker-stopped2","heartbeat_at":"2026-08-26T00:00:00+00:00"}',
            px=5000,
        )
    finally:
        redis.close()
    response = await client.get("/workers")
    item = response.json()["items"][0]
    assert item["status"] == "STOPPED"
    assert item["is_alive"] is False


async def test_malformed_heartbeat_does_not_500(client: AsyncClient) -> None:
    _insert_worker(worker_id="worker-badjson1")
    settings = get_settings()
    redis = redis_sync.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
    )
    try:
        redis.set(heartbeat_key("worker-badjson1"), "garbage", px=5000)
    finally:
        redis.close()
    response = await client.get("/workers")
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["status"] == "UNKNOWN"
    assert item["is_alive"] is None
    assert item["heartbeat_at"] is None


async def test_workers_unknown_when_redis_down(client: AsyncClient) -> None:
    _insert_worker(worker_id="worker-unknown1")
    _insert_worker(worker_id="worker-stopped3", stopped=True)
    redis_stopped = False
    try:
        stop_project_redis()
        redis_stopped = True
        response = await client.get("/workers")
        assert response.status_code == 200
        body = response.json()
        assert body["liveness_available"] is False
        by_id = {item["id"]: item for item in body["items"]}
        assert by_id["worker-unknown1"]["status"] == "UNKNOWN"
        assert by_id["worker-unknown1"]["is_alive"] is None
        assert by_id["worker-stopped3"]["status"] == "STOPPED"
        assert by_id["worker-stopped3"]["is_alive"] is False
    finally:
        if redis_stopped:
            start_project_redis()
