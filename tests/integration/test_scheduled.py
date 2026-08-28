"""Delayed first-run jobs, shared jobs:delayed, and scheduler hardening."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from pathlib import Path

import httpx
import psycopg
import pytest
from httpx import AsyncClient

from job_platform.core.config import get_settings
from job_platform.models.outbox import OUTBOX_EVENT_JOB_INITIAL_SCHEDULE
from job_platform.queue.client import get_redis
from job_platform.scheduler.promoter import promote_due_jobs
from tests.integration.helpers import delayed_members, drain_outbox, stream_entries
from tests.integration.process_harness import (
    ProcessCluster,
    start_project_redis,
    stop_project_redis,
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


async def test_short_delay_does_not_execute_before_run_after(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "later"},
            "delay_seconds": 0.35,
        },
    )
    assert created.status_code == 202
    body = created.json()
    job_id = body["id"]
    assert body["status"] == "SCHEDULED"
    assert body["queued_at"] is None
    assert body["run_after"] is not None
    assert body["attempt_count"] == 0
    run_after = datetime.fromisoformat(body["run_after"])
    await drain_outbox()
    members = await delayed_members()
    assert any(member == job_id for member, _score in members)
    early = (await client.get(f"/jobs/{job_id}")).json()
    assert early["status"] == "SCHEDULED"
    assert early["attempt_count"] == 0
    remaining = run_after.timestamp() - time.time()
    if remaining > 0:
        await asyncio.sleep(remaining + 0.05)
    await promote_due_jobs()
    promoted = (await client.get(f"/jobs/{job_id}")).json()
    assert promoted["status"] == "QUEUED"
    assert promoted["queued_at"] is not None
    assert promoted["run_after"] == body["run_after"]
    assert promoted["attempt_count"] == 0
    from tests.integration.helpers import process_next_message

    await drain_outbox()
    await process_next_message()
    done = (await client.get(f"/jobs/{job_id}")).json()
    assert done["status"] == "SUCCEEDED"
    assert done["run_after"] == body["run_after"]
    assert done["attempt_count"] == 1


async def test_scheduled_high_job_dispatches_to_jobs_high(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "high later"},
            "priority": "HIGH",
            "delay_seconds": 0.2,
        },
    )
    job_id = created.json()["id"]
    run_after = datetime.fromisoformat(created.json()["run_after"])
    await drain_outbox()
    remaining = run_after.timestamp() - time.time()
    if remaining > 0:
        await asyncio.sleep(remaining + 0.05)
    await promote_due_jobs()
    await drain_outbox()
    entries = await stream_entries(get_settings().redis_stream_high)
    assert any(fields["job_id"] == job_id for _mid, fields in entries)
    assert await stream_entries(get_settings().redis_stream_normal) == []


async def test_malformed_delayed_member_is_removed(client: AsyncClient) -> None:
    settings = get_settings()
    await get_redis().zadd(settings.redis_delayed_zset, {"not-a-uuid": time.time() - 10})
    await promote_due_jobs()
    members = await delayed_members()
    assert "not-a-uuid" not in [member for member, _score in members]
    with _pg() as conn:
        count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
    assert count is not None
    assert count[0] == 0


async def test_max_initial_delay_accepted(client: AsyncClient) -> None:
    response = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "week"},
            "delay_seconds": 604800,
        },
    )
    assert response.status_code == 202
    assert response.json()["status"] == "SCHEDULED"


def test_delayed_post_survives_redis_down_and_executes_after_restore(
    redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    redis_stopped = False
    try:
        cluster.start_api()
        stop_project_redis()
        redis_stopped = True
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            created = client.post(
                "/jobs",
                json={
                    "job_type": "word_count",
                    "payload": {"text": "buffered delay"},
                    "delay_seconds": 0.25,
                    "priority": "LOW",
                },
            )
            assert created.status_code == 202
            job_id = created.json()["id"]
            body = client.get(f"/jobs/{job_id}").json()
            assert body["status"] == "SCHEDULED"
            run_after = datetime.fromisoformat(body["run_after"])
        with _pg() as conn:
            event = conn.execute(
                """
                SELECT event_type, published_at FROM outbox_events WHERE job_id = %s
                """,
                (job_id,),
            ).fetchone()
        assert event is not None
        assert event[0] == OUTBOX_EVENT_JOB_INITIAL_SCHEDULE
        assert event[1] is None
        remaining = run_after.timestamp() - time.time()
        if remaining > 0:
            time.sleep(remaining + 0.05)
        cluster.start_publisher()
        cluster.start_scheduler()
        start_project_redis()
        redis_stopped = False
        cluster.start_workers(1)
        deadline = time.monotonic() + 20.0
        status = None
        while time.monotonic() < deadline:
            with _pg() as conn:
                row = conn.execute(
                    "SELECT status, run_after, queued_at FROM jobs WHERE id = %s",
                    (job_id,),
                ).fetchone()
            if row is not None and row[0] == "SUCCEEDED":
                status = row[0]
                assert row[1] is not None
                assert row[2] is not None
                break
            time.sleep(0.05)
        assert status == "SUCCEEDED"
        print(f"DELAYED_REDIS_DOWN job={job_id}")
    finally:
        cluster.stop_all()
        if redis_stopped:
            start_project_redis()


def test_delayed_job_survives_api_and_scheduler_restart(
    redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_publisher()
        cluster.start_scheduler()
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            created = client.post(
                "/jobs",
                json={
                    "job_type": "word_count",
                    "payload": {"text": "restart"},
                    "delay_seconds": 0.8,
                },
            )
            job_id = created.json()["id"]
            run_after = datetime.fromisoformat(created.json()["run_after"])
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline:
            with _pg() as conn:
                row = conn.execute(
                    """
                    SELECT published_at FROM outbox_events
                    WHERE job_id = %s AND event_type = %s
                    """,
                    (job_id, OUTBOX_EVENT_JOB_INITIAL_SCHEDULE),
                ).fetchone()
            if row is not None and row[0] is not None:
                break
            time.sleep(0.05)
        else:
            raise AssertionError("initial schedule never published")
        cluster.stop_api()
        cluster.stop_scheduler()
        remaining = run_after.timestamp() - time.time()
        if remaining > 0:
            time.sleep(remaining + 0.05)
        cluster.start_scheduler()
        cluster.start_workers(1)
        deadline = time.monotonic() + 20.0
        status = None
        while time.monotonic() < deadline:
            with _pg() as conn:
                row = conn.execute("SELECT status FROM jobs WHERE id = %s", (job_id,)).fetchone()
            if row is not None and row[0] == "SUCCEEDED":
                status = row[0]
                break
            time.sleep(0.05)
        assert status == "SUCCEEDED"
    finally:
        cluster.stop_all()


def test_unpublished_initial_schedule_survives_publisher_restart(
    redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            created = client.post(
                "/jobs",
                json={
                    "job_type": "word_count",
                    "payload": {"text": "publisher restart"},
                    "delay_seconds": 0.2,
                },
            )
            job_id = created.json()["id"]
        cluster.start_publisher()
        cluster.stop_publisher()
        cluster.start_publisher()
        cluster.start_scheduler()
        cluster.start_workers(1)
        deadline = time.monotonic() + 20.0
        status = None
        while time.monotonic() < deadline:
            with _pg() as conn:
                row = conn.execute("SELECT status FROM jobs WHERE id = %s", (job_id,)).fetchone()
            if row is not None and row[0] == "SUCCEEDED":
                status = row[0]
                break
            time.sleep(0.05)
        assert status == "SUCCEEDED"
    finally:
        cluster.stop_all()
