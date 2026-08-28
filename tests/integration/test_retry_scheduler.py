"""Retry scheduler process and promotion crash windows."""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime
from pathlib import Path

import httpx
import psycopg
import pytest
from httpx import AsyncClient
from redis.exceptions import RedisError

from job_platform.core.config import get_settings
from job_platform.models.outbox import OUTBOX_EVENT_JOB_DISPATCH
from job_platform.queue.delayed import remove_delayed
from job_platform.scheduler.promoter import promote_due_jobs
from tests.integration.helpers import delayed_members, drain_outbox, process_next_message
from tests.integration.process_harness import (
    ProcessCluster,
    start_project_redis,
    stop_project_redis,
    wait_log_contains,
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


async def test_scheduler_crash_after_commit_before_zrem(
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
    await drain_outbox()
    body = (await client.get(f"/jobs/{job_id}")).json()
    next_retry_at = datetime.fromisoformat(body["next_retry_at"])
    remaining = next_retry_at.timestamp() - time.time()
    if remaining > 0:
        await asyncio.sleep(remaining + 0.05)

    async def fail_zrem(_job_id: uuid.UUID) -> None:
        raise RedisError("injected zrem crash")

    monkeypatch.setattr("job_platform.scheduler.promoter.remove_delayed", fail_zrem)
    await promote_due_jobs()
    promoted = (await client.get(f"/jobs/{job_id}")).json()
    assert promoted["status"] == "QUEUED"
    assert promoted["attempt_count"] == 1
    members = await delayed_members()
    assert any(member == job_id for member, _score in members)
    with _pg() as conn:
        dispatch_count = conn.execute(
            """
            SELECT COUNT(*) FROM outbox_events
            WHERE job_id = %s AND event_type = %s
            """,
            (job_id, OUTBOX_EVENT_JOB_DISPATCH),
        ).fetchone()
    assert dispatch_count is not None
    assert dispatch_count[0] == 2

    monkeypatch.setattr("job_platform.scheduler.promoter.remove_delayed", remove_delayed)
    await promote_due_jobs()
    assert await delayed_members() == []
    with _pg() as conn:
        dispatch_count = conn.execute(
            """
            SELECT COUNT(*) FROM outbox_events
            WHERE job_id = %s AND event_type = %s
            """,
            (job_id, OUTBOX_EVENT_JOB_DISPATCH),
        ).fetchone()
    still = (await client.get(f"/jobs/{job_id}")).json()
    assert still["status"] == "QUEUED"
    assert still["attempt_count"] == 1
    assert dispatch_count is not None
    assert dispatch_count[0] == 2


def test_scheduler_starts_while_redis_is_down_and_resumes_without_restart(
    redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    redis_stopped = False
    try:
        cluster.start_api()
        stop_project_redis()
        redis_stopped = True
        scheduler = cluster.start_scheduler()
        time.sleep(1.5)
        assert cluster.scheduler_alive()
        scheduler_pid = cluster.scheduler_pid()
        assert scheduler.proc.poll() is None
        wait_log_contains(scheduler.log_path, "event=queue_error", timeout=10.0)
        assert cluster.scheduler_alive()
        assert cluster.scheduler_pid() == scheduler_pid

        start_project_redis()
        redis_stopped = False
        assert cluster.scheduler_pid() == scheduler_pid
        assert cluster.scheduler_alive()
        time.sleep(0.5)
        assert cluster.scheduler_pid() == scheduler_pid
        print(f"SCHEDULER_STARTUP_OUTAGE pid={scheduler_pid}")
    finally:
        cluster.stop_all()
        if redis_stopped:
            start_project_redis()


def test_retry_exhaustion_with_real_processes(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_publisher()
        cluster.start_scheduler()
        cluster.start_workers(1)
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            created = client.post(
                "/jobs",
                json={
                    "job_type": "simulate_failure",
                    "payload": {"mode": "retryable"},
                    "max_attempts": 2,
                },
            )
            assert created.status_code == 202
            job_id = created.json()["id"]
            deadline = time.monotonic() + 20.0
            body: dict[str, object] = {}
            while time.monotonic() < deadline:
                body = client.get(f"/jobs/{job_id}").json()
                if body.get("status") == "FAILED":
                    break
                time.sleep(0.05)
            assert body.get("status") == "FAILED"
            assert body.get("attempt_count") == 2
            assert body["error"]["code"] == "MAX_ATTEMPTS_EXHAUSTED"  # type: ignore[index]
    finally:
        cluster.stop_all()
