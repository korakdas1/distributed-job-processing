"""Crash recovery, XAUTOCLAIM, and stale-delivery reconciliation."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import httpx
import psycopg
import pytest
from sqlalchemy import select

from job_platform.core.config import get_settings
from job_platform.db.session import get_session_factory
from job_platform.models.job import JobAttempt
from job_platform.queue.streams import (
    StreamMessage,
    autoclaim_stale,
    ensure_consumer_groups,
    publish_job_id,
    read_one,
)
from job_platform.worker.processor import (
    WorkerOwnershipError,
    _lock_job,
    claim_queued_job,
    persist_terminal_outcome,
    process_message,
    recover_crashed_attempt,
)
from tests.integration.helpers import consumer_pending, drain_outbox, process_next_message
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


def _wait_status(
    client: httpx.Client, job_id: str, status: str, timeout: float = 20.0
) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    body: dict[str, object] = {}
    while time.monotonic() < deadline:
        body = client.get(f"/jobs/{job_id}").json()
        if body.get("status") == status:
            return body
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} never reached {status}: {body}")


async def test_matched_reclaimed_delivery_interrupts_and_creates_new_attempt(
    client: httpx.AsyncClient,
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "recover me"}},
    )
    job_id = uuid.UUID(created.json()["id"])
    await drain_outbox()
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
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "SUCCEEDED"
    assert fetched.json()["attempt_count"] == 2
    with _pg() as conn:
        rows = conn.execute(
            """
            SELECT attempt_number, status, worker_id, delivery_message_id
            FROM job_attempts WHERE job_id = %s ORDER BY attempt_number
            """,
            (str(job_id),),
        ).fetchall()
    assert len(rows) == 2
    assert rows[0][1] == "INTERRUPTED"
    assert rows[0][2] == "worker-aaaa1111"
    assert rows[1][1] == "SUCCEEDED"
    assert rows[1][2] == "worker-bbbb2222"
    assert rows[0][3] == rows[1][3] == message.message_id


async def test_mismatched_reclaimed_delivery_does_not_interrupt(
    client: httpx.AsyncClient,
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "keep running"}},
    )
    job_id = uuid.UUID(created.json()["id"])
    factory = get_session_factory()
    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        await claim_queued_job(
            session,
            job,
            "worker-aaaa1111",
            delivery_stream="jobs:normal",
            delivery_message_id="1-0",
        )
        await session.commit()

    stale = StreamMessage(
        stream="jobs:normal", message_id="9-9", job_id=job_id, fields={"job_id": str(job_id)}
    )
    await process_message("worker-bbbb2222", stale, reclaimed=True)
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "RUNNING"
    assert fetched.json()["worker_id"] == "worker-aaaa1111"
    assert fetched.json()["attempt_count"] == 1
    with _pg() as conn:
        rows = conn.execute(
            "SELECT COUNT(*) FROM job_attempts WHERE job_id = %s",
            (str(job_id),),
        ).fetchone()
    assert rows is not None
    assert rows[0] == 1


async def test_stale_worker_cannot_persist_after_recovery(client: httpx.AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "wakeup"}},
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
        running = await session.scalar(
            select(JobAttempt).where(JobAttempt.id == old_id).with_for_update()
        )
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
            await persist_terminal_outcome(
                session,
                job_id=job_id,
                attempt_id=old_id,
                attempt_number=1,
                worker_id="worker-aaaa1111",
                delivery_stream="jobs:normal",
                delivery_message_id="1-0",
                success=True,
                result={"word_count": 1},
                error=None,
                duration_ms=1,
            )

    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "RUNNING"
    assert fetched.json()["worker_id"] == "worker-bbbb2222"
    assert fetched.json()["attempt_count"] == 2
    assert fetched.json()["result"] is None


async def test_legacy_running_attempt_without_message_id_is_not_guessed(
    client: httpx.AsyncClient,
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "legacy"}},
    )
    job_id = uuid.UUID(created.json()["id"])
    factory = get_session_factory()
    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        attempt = await claim_queued_job(
            session,
            job,
            "worker-aaaa1111",
            delivery_stream="jobs:normal",
            delivery_message_id="1-0",
        )
        await session.commit()
        attempt_id = attempt.id
    with _pg() as conn:
        conn.execute(
            "UPDATE job_attempts SET delivery_message_id = NULL, "
            "delivery_stream = NULL WHERE id = %s",
            (str(attempt_id),),
        )
        conn.commit()

    stale = StreamMessage(
        stream="jobs:normal", message_id="1-0", job_id=job_id, fields={"job_id": str(job_id)}
    )
    await process_message("worker-bbbb2222", stale, reclaimed=True)
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "RUNNING"
    assert fetched.json()["attempt_count"] == 1
    with _pg() as conn:
        status = conn.execute(
            "SELECT status FROM job_attempts WHERE id = %s",
            (str(attempt_id),),
        ).fetchone()
    assert status is not None
    assert status[0] == "RUNNING"


async def test_stale_duplicate_pending_cleaned_after_terminal(
    client: httpx.AsyncClient,
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "once"}},
    )
    job_id = uuid.UUID(created.json()["id"])
    await drain_outbox()
    await publish_job_id(job_id, stream=get_settings().redis_stream_normal)
    await publish_job_id(job_id, stream=get_settings().redis_stream_normal)
    await process_next_message("worker-dup0001")
    await process_next_message("worker-dup0002")
    await process_next_message("worker-dup0003")
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "SUCCEEDED"
    assert fetched.json()["attempt_count"] == 1
    for _ in range(5):
        reclaimed = await autoclaim_stale(
            get_settings().redis_stream_normal,
            "worker-cleaner",
            min_idle_ms=0,
            count=1,
        )
        if reclaimed is None:
            break
        await process_message("worker-cleaner", reclaimed, reclaimed=True)
    assert await consumer_pending() == 0
    with _pg() as conn:
        attempts = conn.execute(
            "SELECT COUNT(*) FROM job_attempts WHERE job_id = %s",
            (str(job_id),),
        ).fetchone()
    assert attempts is not None
    assert attempts[0] == 1


def test_sigkill_worker_is_recovered_by_second_worker(
    redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    cluster.env["JOB_LEASE_TIMEOUT_SECONDS"] = "1"
    cluster.env["WORKER_RECLAIM_INTERVAL_MS"] = "100"
    cluster.env["WORKER_READ_BLOCK_MS"] = "200"
    try:
        cluster.start_api()
        cluster.start_publisher()
        cluster.start_workers(1)
        worker_a_id = cluster.worker_ids()[0]
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            response = client.post(
                "/jobs",
                json={"job_type": "sleep", "payload": {"seconds": 8}},
            )
            assert response.status_code == 202
            job_id = response.json()["id"]
            running = _wait_status(client, job_id, "RUNNING", timeout=10.0)
            assert running["worker_id"] == worker_a_id
            with _pg() as conn:
                attempt1 = conn.execute(
                    """
                    SELECT status, worker_id, delivery_message_id
                    FROM job_attempts WHERE job_id = %s AND attempt_number = 1
                    """,
                    (job_id,),
                ).fetchone()
            assert attempt1 is not None
            assert attempt1[0] == "RUNNING"
            message_id = attempt1[2]
            assert message_id
            cluster.kill_worker_sigkill(0)
            still = client.get(f"/jobs/{job_id}").json()
            assert still["status"] == "RUNNING"
            assert still["attempt_count"] == 1
            worker_b = cluster.start_worker(1)
            wait_log_contains(worker_b.log_path, "event=worker_ready")
            done = _wait_status(client, job_id, "SUCCEEDED", timeout=25.0)
            assert done["attempt_count"] == 2
            assert done["worker_id"] is None
            worker_b_id = cluster.worker_ids()[1]
            assert worker_a_id != worker_b_id
            with _pg() as conn:
                rows = conn.execute(
                    """
                    SELECT attempt_number, status, worker_id, delivery_message_id
                    FROM job_attempts WHERE job_id = %s ORDER BY attempt_number
                    """,
                    (job_id,),
                ).fetchall()
            assert len(rows) == 2
            assert rows[0][1] == "INTERRUPTED"
            assert rows[0][2] == worker_a_id
            assert rows[1][1] == "SUCCEEDED"
            assert rows[1][2] == worker_b_id
            assert rows[0][3] == rows[1][3] == message_id
            settings = get_settings()
            import redis as redis_sync

            client_redis = redis_sync.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                decode_responses=True,
            )
            try:
                info = client_redis.xpending(
                    settings.redis_stream_normal, settings.redis_consumer_group
                )
                pending_after = int(info["pending"] if isinstance(info, dict) else info[0])
            finally:
                client_redis.close()
            assert pending_after == 0
            print(
                f"SIGKILL old={worker_a_id} new={worker_b_id} "
                f"message_id={message_id} attempt1={rows[0][1]} attempt2={rows[1][1]}"
            )
    finally:
        cluster.stop_all()


def test_sigkill_high_priority_worker_is_recovered_from_jobs_high(
    redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    cluster.env["JOB_LEASE_TIMEOUT_SECONDS"] = "1"
    cluster.env["WORKER_RECLAIM_INTERVAL_MS"] = "100"
    cluster.env["WORKER_READ_BLOCK_MS"] = "200"
    try:
        cluster.start_api()
        cluster.start_publisher()
        cluster.start_workers(1)
        worker_a_id = cluster.worker_ids()[0]
        settings = get_settings()
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            response = client.post(
                "/jobs",
                json={
                    "job_type": "sleep",
                    "payload": {"seconds": 8},
                    "priority": "HIGH",
                },
            )
            assert response.status_code == 202
            job_id = response.json()["id"]
            running = _wait_status(client, job_id, "RUNNING", timeout=10.0)
            assert running["worker_id"] == worker_a_id
            with _pg() as conn:
                attempt1 = conn.execute(
                    """
                    SELECT status, worker_id, delivery_message_id, delivery_stream
                    FROM job_attempts WHERE job_id = %s AND attempt_number = 1
                    """,
                    (job_id,),
                ).fetchone()
            assert attempt1 is not None
            assert attempt1[0] == "RUNNING"
            message_id = attempt1[2]
            assert message_id
            assert attempt1[3] == settings.redis_stream_high
            cluster.kill_worker_sigkill(0)
            worker_b = cluster.start_worker(1)
            wait_log_contains(worker_b.log_path, "event=worker_ready")
            done = _wait_status(client, job_id, "SUCCEEDED", timeout=25.0)
            assert done["attempt_count"] == 2
            worker_b_id = cluster.worker_ids()[1]
            with _pg() as conn:
                rows = conn.execute(
                    """
                    SELECT attempt_number, status, worker_id,
                           delivery_message_id, delivery_stream
                    FROM job_attempts WHERE job_id = %s ORDER BY attempt_number
                    """,
                    (job_id,),
                ).fetchall()
            assert len(rows) == 2
            assert rows[0][1] == "INTERRUPTED"
            assert rows[0][2] == worker_a_id
            assert rows[1][1] == "SUCCEEDED"
            assert rows[1][2] == worker_b_id
            assert rows[0][3] == rows[1][3] == message_id
            assert rows[0][4] == rows[1][4] == settings.redis_stream_high
            import redis as redis_sync

            client_redis = redis_sync.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                decode_responses=True,
            )
            try:
                info = client_redis.xpending(
                    settings.redis_stream_high, settings.redis_consumer_group
                )
                pending_after = int(info["pending"] if isinstance(info, dict) else info[0])
            finally:
                client_redis.close()
            assert pending_after == 0
            deadline = time.monotonic() + 5.0
            expired = None
            active = None
            while time.monotonic() < deadline:
                listed = client.get("/workers").json()
                by_id = {item["id"]: item for item in listed["items"]}
                expired = by_id.get(worker_a_id, {}).get("status")
                active = by_id.get(worker_b_id, {}).get("status")
                if expired == "EXPIRED" and active == "ACTIVE":
                    break
                time.sleep(0.05)
            assert expired == "EXPIRED"
            assert active == "ACTIVE"
            print(
                f"HIGH_SIGKILL old={worker_a_id} new={worker_b_id} "
                f"stream={settings.redis_stream_high} message_id={message_id}"
            )
            print(f"CRASH_VISIBILITY old={worker_a_id} EXPIRED new={worker_b_id} ACTIVE")
    finally:
        cluster.stop_all()
