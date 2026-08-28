"""Transactional outbox publisher tests."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import httpx
import psycopg
import pytest
from httpx import AsyncClient
from redis.exceptions import RedisError

from job_platform.api.services import create_dispatch_outbox_event, create_job_record
from job_platform.core.config import get_settings
from job_platform.core.enums import JobPriority
from job_platform.db.session import dispose_engine, get_session_factory
from job_platform.outbox.publisher import publish_available
from job_platform.queue.streams import publish_job_id
from tests.integration.helpers import drain_outbox, process_next_message, stream_entries
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


async def test_job_and_outbox_inserted_together(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "atomic"}},
    )
    assert created.status_code == 202
    job_id = created.json()["id"]
    with _pg() as conn:
        jobs = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
        events = conn.execute("SELECT COUNT(*) FROM outbox_events").fetchone()
        event = conn.execute(
            """
            SELECT event_type, published_at, redis_message_id, publish_attempts
            FROM outbox_events WHERE job_id = %s
            """,
            (job_id,),
        ).fetchone()
    assert jobs is not None and jobs[0] == 1
    assert events is not None and events[0] == 1
    assert event is not None
    assert event[0] == "JOB_DISPATCH"
    assert event[1] is None
    assert event[2] is None
    assert event[3] == 0


async def test_same_transaction_rollback_inserts_neither_job_nor_outbox() -> None:
    settings = get_settings()
    job = create_job_record(
        settings=settings,
        job_type="word_count",
        payload={"text": "rollback"},
        priority=JobPriority.NORMAL,
        max_attempts=None,
        timeout_seconds=None,
        delay_seconds=0,
    )
    event = create_dispatch_outbox_event(job)
    factory = get_session_factory()
    async with factory() as session:
        session.add(job)
        session.add(event)
        await session.flush()
        await session.rollback()
    with _pg() as conn:
        jobs = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
        events = conn.execute("SELECT COUNT(*) FROM outbox_events").fetchone()
    assert jobs is not None and jobs[0] == 0
    assert events is not None and events[0] == 0


async def test_publisher_replays_after_redis_returns(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "replay"}},
    )
    assert created.status_code == 202
    job_id = created.json()["id"]
    assert await stream_entries() == []
    published = await drain_outbox()
    assert published == 1
    with _pg() as conn:
        event = conn.execute(
            "SELECT published_at, redis_message_id FROM outbox_events WHERE job_id = %s",
            (job_id,),
        ).fetchone()
    assert event is not None
    assert event[0] is not None
    assert event[1]
    await process_next_message()
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "SUCCEEDED"


async def test_api_restart_does_not_lose_unpublished_outbox(
    client: AsyncClient,
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "survive api"}},
    )
    job_id = created.json()["id"]
    await dispose_engine()
    await drain_outbox()
    await process_next_message()
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "SUCCEEDED"


async def test_publisher_restart_drains_backlog(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "backlog"}},
    )
    job_id = created.json()["id"]
    first = await publish_available(limit=1)
    assert first == 1
    second = await publish_available(limit=1)
    assert second == 0
    await process_next_message()
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "SUCCEEDED"


async def test_outbox_duplicate_xadd_window_still_one_logical_job(
    client: AsyncClient,
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "dup publish"}},
    )
    job_id = created.json()["id"]
    await publish_job_id(uuid.UUID(job_id), stream=get_settings().redis_stream_normal)
    await drain_outbox()
    entries = await stream_entries()
    assert len(entries) >= 2
    await process_next_message()
    await process_next_message()
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "SUCCEEDED"
    assert fetched.json()["attempt_count"] == 1
    with _pg() as conn:
        attempts = conn.execute(
            "SELECT COUNT(*) FROM job_attempts WHERE job_id = %s",
            (job_id,),
        ).fetchone()
    assert attempts is not None
    assert attempts[0] == 1


async def test_publisher_publishes_unpublished_events_in_created_at_order(
    client: AsyncClient,
) -> None:
    first = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "first"}},
    )
    second = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "second"}},
    )
    assert await drain_outbox() == 2
    entries = await stream_entries()
    assert [fields["job_id"] for _mid, fields in entries] == [
        first.json()["id"],
        second.json()["id"],
    ]


async def test_publisher_redis_failure_keeps_unpublished_event(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "hold"}},
    )
    job_id = created.json()["id"]

    async def fail_publish(*_args: object, **_kwargs: object) -> str:
        raise RedisError("injected redis outage")

    monkeypatch.setattr("job_platform.outbox.publisher.publish_job_id", fail_publish)
    with pytest.raises(RedisError):
        await publish_available(limit=1)
    with _pg() as conn:
        event = conn.execute(
            """
            SELECT published_at, redis_message_id, publish_attempts, last_error
            FROM outbox_events WHERE job_id = %s
            """,
            (job_id,),
        ).fetchone()
    assert event is not None
    assert event[0] is None
    assert event[1] is None
    assert event[2] >= 1
    assert event[3] is not None
    assert "injected redis outage" in event[3]
    assert await stream_entries() == []


async def test_publisher_crash_after_xadd_before_commit(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "crash window"}},
    )
    job_id = created.json()["id"]
    real_publish = publish_job_id

    async def crash_after_xadd(
        published_job_id: uuid.UUID,
        *,
        stream: str,
        outbox_event_id: uuid.UUID | None = None,
    ) -> str:
        await real_publish(published_job_id, stream=stream, outbox_event_id=outbox_event_id)
        raise RuntimeError("injected crash after XADD")

    monkeypatch.setattr("job_platform.outbox.publisher.publish_job_id", crash_after_xadd)
    with pytest.raises(RuntimeError, match="injected crash after XADD"):
        await publish_available(limit=1)
    with _pg() as conn:
        event = conn.execute(
            "SELECT published_at FROM outbox_events WHERE job_id = %s",
            (job_id,),
        ).fetchone()
    assert event is not None
    assert event[0] is None
    assert len(await stream_entries()) == 1

    monkeypatch.setattr("job_platform.outbox.publisher.publish_job_id", real_publish)
    assert await drain_outbox() == 1
    assert len(await stream_entries()) >= 2
    await process_next_message()
    await process_next_message()
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "SUCCEEDED"
    assert fetched.json()["attempt_count"] == 1


def test_publisher_and_worker_run_without_api(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            created = client.post(
                "/jobs",
                json={"job_type": "word_count", "payload": {"text": "no api"}},
            )
            assert created.status_code == 202
            job_id = created.json()["id"]
        cluster.stop_api()
        cluster.start_publisher()
        cluster.start_workers(1)
        deadline = time.monotonic() + 15.0
        status = None
        while time.monotonic() < deadline:
            with _pg() as conn:
                row = conn.execute(
                    "SELECT status FROM jobs WHERE id = %s",
                    (job_id,),
                ).fetchone()
            if row is not None and row[0] == "SUCCEEDED":
                status = row[0]
                break
            time.sleep(0.05)
        assert status == "SUCCEEDED"
    finally:
        cluster.stop_all()


def test_publisher_starts_while_redis_is_down_and_resumes_without_restart(
    redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    redis_stopped = False
    try:
        cluster.start_api()
        stop_project_redis()
        redis_stopped = True
        publisher = cluster.start_publisher()
        time.sleep(1.5)
        assert cluster.publisher_alive()
        publisher_pid = cluster.publisher_pid()
        assert publisher.proc.poll() is None

        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            health = client.get("/health")
            assert health.status_code == 200
            ready = client.get("/ready")
            assert ready.status_code == 200
            created = client.post(
                "/jobs",
                json={"job_type": "word_count", "payload": {"text": "startup outage"}},
            )
            assert created.status_code == 202
            job_id = created.json()["id"]
            queued = client.get(f"/jobs/{job_id}").json()
            assert queued["status"] == "QUEUED"

        wait_log_contains(publisher.log_path, "event=outbox_publish_failed", timeout=10.0)
        with _pg() as conn:
            event = conn.execute(
                "SELECT published_at FROM outbox_events WHERE job_id = %s",
                (job_id,),
            ).fetchone()
        assert event is not None
        assert event[0] is None
        assert cluster.publisher_alive()
        assert cluster.publisher_pid() == publisher_pid

        start_project_redis()
        redis_stopped = False
        assert cluster.publisher_pid() == publisher_pid
        assert cluster.publisher_alive()

        published_at = None
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            assert cluster.publisher_pid() == publisher_pid
            with _pg() as conn:
                row = conn.execute(
                    "SELECT published_at FROM outbox_events WHERE job_id = %s",
                    (job_id,),
                ).fetchone()
            if row is not None and row[0] is not None:
                published_at = row[0]
                break
            time.sleep(0.1)
        assert published_at is not None
        assert cluster.publisher_pid() == publisher_pid

        cluster.start_workers(1)
        status = None
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            with _pg() as conn:
                row = conn.execute(
                    "SELECT status FROM jobs WHERE id = %s",
                    (job_id,),
                ).fetchone()
            if row is not None and row[0] == "SUCCEEDED":
                status = row[0]
                break
            time.sleep(0.05)
        assert status == "SUCCEEDED"
        print(f"PUBLISHER_STARTUP_OUTAGE pid={publisher_pid} job={job_id}")
    finally:
        cluster.stop_all()
        if redis_stopped:
            start_project_redis()


def test_message_published_before_first_worker_is_still_consumed(
    redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_publisher()
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            created = client.post(
                "/jobs",
                json={"job_type": "word_count", "payload": {"text": "before worker"}},
            )
            assert created.status_code == 202
            job_id = created.json()["id"]
        published_at = None
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            with _pg() as conn:
                row = conn.execute(
                    "SELECT published_at FROM outbox_events WHERE job_id = %s",
                    (job_id,),
                ).fetchone()
            if row is not None and row[0] is not None:
                published_at = row[0]
                break
            time.sleep(0.05)
        assert published_at is not None
        cluster.start_workers(1)
        status = None
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            with _pg() as conn:
                row = conn.execute(
                    "SELECT status FROM jobs WHERE id = %s",
                    (job_id,),
                ).fetchone()
            if row is not None and row[0] == "SUCCEEDED":
                status = row[0]
                break
            time.sleep(0.05)
        assert status == "SUCCEEDED"
    finally:
        cluster.stop_all()


def test_publisher_idles_quietly_with_empty_outbox(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        publisher = cluster.start_publisher()
        time.sleep(1.5)
        assert cluster.publisher_alive()
        logs = publisher.log_path.read_text(encoding="utf-8")
        assert "event=publisher_ready" in logs
        assert logs.count("event=publisher_ready") == 1
    finally:
        cluster.stop_all()
