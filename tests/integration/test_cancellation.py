"""Cooperative cancellation: waiting jobs, RUNNING, crash recovery, Redis-down."""

from __future__ import annotations

import asyncio
import time
import uuid
from pathlib import Path

import httpx
import psycopg
import pytest
from httpx import AsyncClient

from job_platform.core.config import get_settings
from job_platform.core.enums import AttemptStatus
from job_platform.db.session import dispose_engine, get_session_factory
from job_platform.queue.client import dispose_redis
from job_platform.queue.streams import ensure_consumer_groups
from job_platform.scheduler.promoter import promote_due_jobs
from job_platform.worker.processor import _lock_job, claim_queued_job, persist_handler_failure
from tests.integration.helpers import (
    consumer_pending,
    delayed_members,
    drain_outbox,
    process_next_message,
)
from tests.integration.process_harness import ProcessCluster, wait_log_contains

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


def _event_count(job_id: str, event_type: str) -> int:
    with _pg() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM outbox_events WHERE job_id = %s AND event_type = %s",
            (job_id, event_type),
        ).fetchone()
    assert row is not None
    return int(row[0])


async def _wait_status(
    client: AsyncClient, job_id: str, status: str, timeout: float = 15.0
) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    body: dict[str, object] = {}
    while time.monotonic() < deadline:
        body = (await client.get(f"/jobs/{job_id}")).json()
        if body.get("status") == status:
            return body
        await asyncio.sleep(0.05)
    raise AssertionError(f"job {job_id} never reached {status}: {body}")


async def test_cancel_unknown_job(client: AsyncClient) -> None:
    missing = uuid.uuid4()
    response = await client.delete(f"/jobs/{missing}")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "JOB_NOT_FOUND"


async def test_cancel_scheduled_job(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "later"}, "delay_seconds": 30},
    )
    job_id = created.json()["id"]
    await drain_outbox()
    assert await delayed_members()
    cancelled = await client.delete(f"/jobs/{job_id}")
    assert cancelled.status_code == 200
    body = cancelled.json()
    assert body["status"] == "CANCELLED"
    assert body["cancel_requested_at"] is not None
    assert body["cancelled_at"] is not None
    assert body["completed_at"] is not None
    assert body["attempt_count"] == 0
    assert body["error"] is None
    await promote_due_jobs()
    assert (await client.get(f"/jobs/{job_id}")).json()["status"] == "CANCELLED"
    with _pg() as conn:
        attempts = conn.execute(
            "SELECT COUNT(*) FROM job_attempts WHERE job_id = %s", (job_id,)
        ).fetchone()
    assert attempts is not None and int(attempts[0]) == 0
    assert _event_count(job_id, "JOB_RETRY_SCHEDULE") == 0
    assert _event_count(job_id, "JOB_DEAD_LETTER") == 0
    print(f"CANCEL_SCHEDULED job={job_id}")


async def test_cancel_queued_stale_delivery(client: AsyncClient) -> None:
    created = await client.post("/jobs", json={"job_type": "word_count", "payload": {"text": "q"}})
    job_id = created.json()["id"]
    await drain_outbox()
    cancelled = await client.delete(f"/jobs/{job_id}")
    assert cancelled.status_code == 200
    assert cancelled.json()["attempt_count"] == 0
    await process_next_message()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "CANCELLED"
    with _pg() as conn:
        attempts = conn.execute(
            "SELECT COUNT(*) FROM job_attempts WHERE job_id = %s", (job_id,)
        ).fetchone()
    assert attempts is not None and int(attempts[0]) == 0
    print(f"CANCEL_QUEUED job={job_id}")


async def test_cancel_retrying_job(client: AsyncClient) -> None:
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
    cancelled = await client.delete(f"/jobs/{job_id}")
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "CANCELLED"
    assert cancelled.json()["next_retry_at"] is None
    assert cancelled.json()["attempt_count"] == 1
    await drain_outbox()
    await promote_due_jobs()
    await process_next_message()
    final = (await client.get(f"/jobs/{job_id}")).json()
    assert final["status"] == "CANCELLED"
    assert final["attempt_count"] == 1
    assert _event_count(job_id, "JOB_DEAD_LETTER") == 0
    assert _event_count(job_id, "JOB_RETRY_SCHEDULE") == 1
    print(f"CANCEL_RETRYING job={job_id}")


async def test_cancel_running_sleep(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "sleep", "payload": {"seconds": 8}},
    )
    job_id = created.json()["id"]
    task = asyncio.create_task(process_next_message())
    await _wait_status(client, job_id, "RUNNING")
    started = time.monotonic()
    requested = await client.delete(f"/jobs/{job_id}")
    assert requested.status_code == 202
    pending = requested.json()
    assert pending["status"] == "RUNNING"
    assert pending["cancel_requested_at"] is not None
    assert pending["cancelled_at"] is None
    await task
    elapsed = time.monotonic() - started
    final = (await client.get(f"/jobs/{job_id}")).json()
    assert final["status"] == "CANCELLED"
    assert final["cancelled_at"] is not None
    assert final["completed_at"] is not None
    assert final["worker_id"] is None
    assert elapsed < 4.0
    with _pg() as conn:
        attempt = conn.execute(
            "SELECT status FROM job_attempts WHERE job_id = %s AND attempt_number = 1",
            (job_id,),
        ).fetchone()
    assert attempt is not None and attempt[0] == AttemptStatus.CANCELLED.value
    assert await consumer_pending() == 0
    print(f"CANCEL_RUNNING job={job_id} elapsed_s={elapsed:.3f}")


async def test_cancel_already_cancelled_is_idempotent(client: AsyncClient) -> None:
    created = await client.post("/jobs", json={"job_type": "word_count", "payload": {"text": "x"}})
    job_id = created.json()["id"]
    first = await client.delete(f"/jobs/{job_id}")
    second = await client.delete(f"/jobs/{job_id}")
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["cancel_requested_at"] == second.json()["cancel_requested_at"]
    assert first.json()["cancelled_at"] == second.json()["cancelled_at"]


async def test_cancel_succeeded_conflict(client: AsyncClient) -> None:
    created = await client.post("/jobs", json={"job_type": "word_count", "payload": {"text": "ok"}})
    job_id = created.json()["id"]
    await process_next_message()
    response = await client.delete(f"/jobs/{job_id}")
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "JOB_NOT_CANCELLABLE"
    assert (await client.get(f"/jobs/{job_id}")).json()["status"] == "SUCCEEDED"


async def test_cancel_failed_conflict(client: AsyncClient) -> None:
    created = await client.post("/jobs", json={"job_type": "simulate_failure", "payload": {}})
    job_id = created.json()["id"]
    await process_next_message()
    response = await client.delete(f"/jobs/{job_id}")
    assert response.status_code == 409
    assert (await client.get(f"/jobs/{job_id}")).json()["status"] == "FAILED"


async def test_list_cancelled_filter_and_metric(client: AsyncClient) -> None:
    created = await client.post("/jobs", json={"job_type": "word_count", "payload": {"text": "m"}})
    job_id = created.json()["id"]
    await client.delete(f"/jobs/{job_id}")
    listed = await client.get("/jobs", params={"status": "CANCELLED"})
    ids = [item["id"] for item in listed.json()["items"]]
    assert job_id in ids
    metrics = await client.get("/metrics")
    from tests.integration.test_metrics import sample_float

    assert sample_float(metrics.text, "job_platform_jobs", status="CANCELLED") >= 1


async def test_cancel_wins_over_retryable_failure(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "race"}, "max_attempts": 5},
    )
    job_id = uuid.UUID(created.json()["id"])
    await drain_outbox()
    await ensure_consumer_groups()
    factory = get_session_factory()
    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        attempt = await claim_queued_job(
            session,
            job,
            "worker-cancel1",
            delivery_stream=get_settings().redis_stream_normal,
            delivery_message_id="1-0",
        )
        await session.commit()
        attempt_id = attempt.id
    requested = await client.delete(f"/jobs/{job_id}")
    assert requested.status_code == 202
    async with factory() as session:
        await persist_handler_failure(
            session,
            job_id=job_id,
            attempt_id=attempt_id,
            attempt_number=1,
            worker_id="worker-cancel1",
            delivery_stream=get_settings().redis_stream_normal,
            delivery_message_id="1-0",
            error={"code": "SIMULATED_RETRYABLE_FAILURE", "message": "x", "retryable": True},
            duration_ms=10,
        )
        await session.commit()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "CANCELLED"
    assert body["next_retry_at"] is None
    assert _event_count(str(job_id), "JOB_RETRY_SCHEDULE") == 0
    assert _event_count(str(job_id), "JOB_DEAD_LETTER") == 0
    with _pg() as conn:
        attempt = conn.execute(
            "SELECT status FROM job_attempts WHERE job_id = %s", (str(job_id),)
        ).fetchone()
    assert attempt is not None and attempt[0] == "CANCELLED"
    print(f"CANCEL_BEATS_RETRY job={job_id}")


async def test_cancel_waiting_job_with_redis_down(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = await client.post("/jobs", json={"job_type": "word_count", "payload": {"text": "r"}})
    job_id = created.json()["id"]
    original = str(get_settings().redis_port)
    monkeypatch.setenv("REDIS_PORT", "1")
    get_settings.cache_clear()
    await dispose_engine()
    await dispose_redis()
    cancelled = await client.delete(f"/jobs/{job_id}")
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "CANCELLED"
    monkeypatch.setenv("REDIS_PORT", original)
    get_settings.cache_clear()
    await dispose_redis()


async def test_cancel_running_request_with_redis_down(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = await client.post(
        "/jobs", json={"job_type": "word_count", "payload": {"text": "run"}}
    )
    job_id = uuid.UUID(created.json()["id"])
    factory = get_session_factory()
    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        await claim_queued_job(
            session,
            job,
            "worker-redis-down",
            delivery_stream=get_settings().redis_stream_normal,
            delivery_message_id="9-0",
        )
        await session.commit()
    original = str(get_settings().redis_port)
    monkeypatch.setenv("REDIS_PORT", "1")
    get_settings.cache_clear()
    await dispose_engine()
    await dispose_redis()
    requested = await client.delete(f"/jobs/{job_id}")
    assert requested.status_code == 202
    assert requested.json()["status"] == "RUNNING"
    assert requested.json()["cancel_requested_at"] is not None
    monkeypatch.setenv("REDIS_PORT", original)
    get_settings.cache_clear()
    await dispose_redis()


def test_cancel_requested_then_sigkill_does_not_create_attempt_two(
    redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    cluster.env["JOB_LEASE_TIMEOUT_SECONDS"] = "1"
    cluster.env["WORKER_RECLAIM_INTERVAL_MS"] = "100"
    cluster.env["WORKER_CANCELLATION_POLL_INTERVAL_MS"] = "5000"
    try:
        cluster.start_api()
        cluster.start_publisher()
        cluster.start_workers(1)
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            response = client.post(
                "/jobs",
                json={"job_type": "sleep", "payload": {"seconds": 8}, "priority": "HIGH"},
            )
            assert response.status_code == 202
            job_id = response.json()["id"]
            deadline = time.monotonic() + 10.0
            body: dict = {}
            while time.monotonic() < deadline:
                body = client.get(f"/jobs/{job_id}").json()
                if body.get("status") == "RUNNING":
                    break
                time.sleep(0.05)
            assert body.get("status") == "RUNNING"
            requested = client.delete(f"/jobs/{job_id}")
            assert requested.status_code == 202
            cluster.kill_worker_sigkill(0)
            worker_b = cluster.start_worker(1)
            wait_log_contains(worker_b.log_path, "event=worker_ready")
            deadline = time.monotonic() + 25.0
            final: dict = {}
            while time.monotonic() < deadline:
                final = client.get(f"/jobs/{job_id}").json()
                if final.get("status") == "CANCELLED":
                    break
                time.sleep(0.05)
            assert final.get("status") == "CANCELLED"
            assert final.get("attempt_count") == 1
            with _pg() as conn:
                rows = conn.execute(
                    """
                    SELECT attempt_number, status FROM job_attempts
                    WHERE job_id = %s ORDER BY attempt_number
                    """,
                    (job_id,),
                ).fetchall()
            assert len(rows) == 1
            assert rows[0][1] == "INTERRUPTED"
            print(f"CANCEL_CRASH job={job_id} attempt1=INTERRUPTED")
    finally:
        cluster.stop_all()
