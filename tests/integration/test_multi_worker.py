"""Multi-worker process concurrency and claim-safety tests."""

from __future__ import annotations

import asyncio
import time
import uuid
from pathlib import Path

import httpx
import psycopg
import pytest
import redis as redis_sync
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from job_platform.core.clock import utcnow
from job_platform.core.config import get_settings
from job_platform.core.enums import AttemptStatus, JobStatus
from job_platform.db.session import get_session_factory
from job_platform.models.job import JobAttempt
from job_platform.queue.streams import ensure_consumer_groups, publish_job_id
from job_platform.tasks.handlers import execute_handler as real_execute_handler
from job_platform.worker.processor import (
    WorkerOwnershipError,
    _lock_job,
    claim_queued_job,
    persist_terminal_outcome,
)
from tests.integration.helpers import consumer_pending, process_next_message
from tests.integration.process_harness import ProcessCluster, stop_process

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


def _wait_jobs(
    client: httpx.Client,
    job_ids: list[str],
    *,
    terminal: str = "SUCCEEDED",
    timeout: float = 45.0,
) -> list[dict[str, object]]:
    deadline = time.monotonic() + timeout
    bodies: list[dict[str, object]] = []
    while time.monotonic() < deadline:
        bodies = [client.get(f"/jobs/{job_id}").json() for job_id in job_ids]
        if all(item["status"] == terminal for item in bodies):
            return bodies
        time.sleep(0.05)
    statuses = [item.get("status") for item in bodies]
    raise AssertionError(f"Jobs did not reach {terminal}: {statuses}")


def _consumer_names_sync() -> list[str]:
    settings = get_settings()
    client = redis_sync.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
        socket_connect_timeout=2,
    )
    try:
        info = client.xinfo_consumers(
            settings.redis_stream_normal,
            settings.redis_consumer_group,
        )
    except redis_sync.ResponseError:
        return []
    finally:
        client.close()
    names: list[str] = []
    for item in info:
        name = str(item.get("name") or "") if isinstance(item, dict) else str(item[0])
        if name:
            names.append(name)
    return names


async def test_simultaneous_consumer_group_create() -> None:
    await asyncio.gather(*[ensure_consumer_groups() for _ in range(12)])


async def test_two_sessions_cannot_double_claim(client: httpx.AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "lock race"}},
    )
    job_id = uuid.UUID(created.json()["id"])
    factory = get_session_factory()
    first_locked = asyncio.Event()
    results: list[str] = []

    async def first_claim() -> None:
        async with factory() as session:
            job = await _lock_job(session, job_id)
            assert job is not None
            first_locked.set()
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                blocked = await session.scalar(
                    text("SELECT COUNT(*) FROM pg_locks WHERE NOT granted")
                )
                if int(blocked or 0) > 0:
                    break
                await asyncio.sleep(0.02)
            else:
                raise AssertionError("second session never waited on the row lock")
            attempt = await claim_queued_job(
                session,
                job,
                "worker-first01",
                delivery_stream="jobs:normal",
                delivery_message_id="1-0",
            )
            await session.commit()
            results.append(f"claimed:{attempt.attempt_number}")

    async def second_claim() -> None:
        await first_locked.wait()
        async with factory() as session:
            job = await _lock_job(session, job_id)
            assert job is not None
            status = JobStatus(job.status)
            if status is JobStatus.QUEUED:
                await claim_queued_job(
                    session,
                    job,
                    "worker-second2",
                    delivery_stream="jobs:normal",
                    delivery_message_id="1-0",
                )
                await session.commit()
                results.append("second-claimed")
            else:
                await session.rollback()
                results.append(f"second-saw:{status.value}")

    await asyncio.gather(first_claim(), second_claim())
    assert "claimed:1" in results
    assert "second-claimed" not in results
    assert any(item.startswith("second-saw:RUNNING") for item in results)

    with _pg() as conn:
        job_row = conn.execute(
            "SELECT status, attempt_count, worker_id FROM jobs WHERE id = %s",
            (str(job_id),),
        ).fetchone()
        attempts = conn.execute(
            "SELECT COUNT(*) FROM job_attempts WHERE job_id = %s",
            (str(job_id),),
        ).fetchone()
    assert job_row is not None
    assert job_row[0] == "RUNNING"
    assert job_row[1] == 1
    assert job_row[2] == "worker-first01"
    assert attempts is not None
    assert attempts[0] == 1


async def test_stale_job_worker_id_cannot_persist(client: httpx.AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "stale owner"}},
    )
    job_id = uuid.UUID(created.json()["id"])
    factory = get_session_factory()
    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        attempt = await claim_queued_job(
            session, job, "worker-owner01", delivery_stream="jobs:normal", delivery_message_id="1-0"
        )
        await session.commit()
        attempt_id = attempt.id

    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        job.worker_id = "worker-other02"
        await session.commit()

    async with factory() as session:
        with pytest.raises(WorkerOwnershipError):
            await persist_terminal_outcome(
                session,
                job_id=job_id,
                attempt_id=attempt_id,
                attempt_number=1,
                worker_id="worker-owner01",
                delivery_stream="jobs:normal",
                delivery_message_id="1-0",
                success=True,
                result={"word_count": 2},
                error=None,
                duration_ms=1,
            )

    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "RUNNING"
    assert fetched.json()["result"] is None
    assert fetched.json()["worker_id"] == "worker-other02"


async def test_stale_attempt_worker_id_cannot_persist(client: httpx.AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "stale attempt"}},
    )
    job_id = uuid.UUID(created.json()["id"])
    factory = get_session_factory()
    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        attempt = await claim_queued_job(
            session, job, "worker-owner01", delivery_stream="jobs:normal", delivery_message_id="1-0"
        )
        await session.commit()
        attempt_id = attempt.id

    with _pg() as conn:
        conn.execute(
            "UPDATE job_attempts SET worker_id = %s WHERE id = %s",
            ("worker-other02", str(attempt_id)),
        )
        conn.commit()

    async with factory() as session:
        with pytest.raises(WorkerOwnershipError):
            await persist_terminal_outcome(
                session,
                job_id=job_id,
                attempt_id=attempt_id,
                attempt_number=1,
                worker_id="worker-owner01",
                delivery_stream="jobs:normal",
                delivery_message_id="1-0",
                success=True,
                result={"word_count": 2},
                error=None,
                duration_ms=1,
            )

    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "RUNNING"
    assert fetched.json()["result"] is None


async def test_stale_attempt_number_cannot_persist(client: httpx.AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "stale number"}},
    )
    job_id = uuid.UUID(created.json()["id"])
    factory = get_session_factory()
    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        attempt = await claim_queued_job(
            session, job, "worker-owner01", delivery_stream="jobs:normal", delivery_message_id="1-0"
        )
        await session.commit()
        attempt_id = attempt.id

    with _pg() as conn:
        conn.execute("UPDATE jobs SET attempt_count = 2 WHERE id = %s", (str(job_id),))
        conn.commit()

    async with factory() as session:
        with pytest.raises(WorkerOwnershipError):
            await persist_terminal_outcome(
                session,
                job_id=job_id,
                attempt_id=attempt_id,
                attempt_number=1,
                worker_id="worker-owner01",
                delivery_stream="jobs:normal",
                delivery_message_id="1-0",
                success=True,
                result={"word_count": 1},
                error=None,
                duration_ms=1,
            )

    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "RUNNING"


async def test_duplicate_job_id_race_executes_handler_once(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "once"}},
    )
    job_id = uuid.UUID(created.json()["id"])
    await ensure_consumer_groups()
    await publish_job_id(job_id, stream=get_settings().redis_stream_normal)
    await publish_job_id(job_id, stream=get_settings().redis_stream_normal)

    starts: list[str] = []

    async def counted(
        job_type: object, payload: object, context: object = None
    ) -> dict[str, object]:
        starts.append("start")
        return await real_execute_handler(job_type, payload, context)  # type: ignore[arg-type]

    monkeypatch.setattr("job_platform.worker.processor.execute_handler", counted)

    await asyncio.gather(
        process_next_message("worker-dup0001"),
        process_next_message("worker-dup0002"),
        process_next_message("worker-dup0003"),
    )
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "SUCCEEDED"
    assert fetched.json()["attempt_count"] == 1
    assert fetched.json()["result"] == {"word_count": 1}
    assert len(starts) == 1
    with _pg() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM job_attempts WHERE job_id = %s",
            (str(job_id),),
        ).fetchone()
    assert count is not None
    assert count[0] == 1
    pending = await consumer_pending()
    print(
        f"DUP_RACE handler_starts={len(starts)} attempts={count[0]} "
        f"pending={pending} (stale pending duplicates may remain until later XACK)"
    )


async def test_attempt_unique_constraint_still_enforced(client: httpx.AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "unique"}},
    )
    job_id = uuid.UUID(created.json()["id"])
    factory = get_session_factory()
    async with factory() as session:
        job = await _lock_job(session, job_id)
        assert job is not None
        await claim_queued_job(
            session, job, "worker-owner01", delivery_stream="jobs:normal", delivery_message_id="1-0"
        )
        await session.commit()

    async with factory() as session:
        session.add(
            JobAttempt(
                id=uuid.uuid4(),
                job_id=job_id,
                attempt_number=1,
                worker_id="worker-other02",
                started_at=utcnow(),
                status=AttemptStatus.RUNNING.value,
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()


def test_four_workers_process_one_hundred_jobs(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_publisher()
        cluster.start_scheduler()
        cluster.start_workers(4)
        pids = cluster.worker_pids()
        worker_ids = cluster.worker_ids()
        assert len(set(pids)) == 4
        assert len(set(worker_ids)) == 4
        assert cluster.api_pid() not in set(pids)

        deadline = time.monotonic() + 10.0
        names: list[str] = []
        while time.monotonic() < deadline:
            names = _consumer_names_sync()
            if len(set(names)) >= 4:
                break
            time.sleep(0.05)
        assert len(set(names)) >= 4, names

        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            job_ids: list[str] = []
            priorities = ["CRITICAL", "HIGH", "NORMAL", "LOW"]
            for index in range(100):
                response = client.post(
                    "/jobs",
                    json={
                        "job_type": "word_count",
                        "payload": {"text": f"job {index}"},
                        "priority": priorities[index % 4],
                    },
                )
                assert response.status_code == 202, response.text
                job_ids.append(response.json()["id"])
            assert len(set(job_ids)) == 100
            bodies = _wait_jobs(client, job_ids, timeout=60.0)
            assert all(item["status"] == "SUCCEEDED" for item in bodies)
            assert all(item["attempt_count"] == 1 for item in bodies)
            assert all(item["worker_id"] is None for item in bodies)

        with _pg() as conn:
            jobs = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
            attempts = conn.execute("SELECT COUNT(*) FROM job_attempts").fetchone()
            distinct = conn.execute("SELECT COUNT(DISTINCT job_id) FROM job_attempts").fetchone()
            max_attempt = conn.execute("SELECT MAX(attempt_number) FROM job_attempts").fetchone()
            dist_rows = conn.execute(
                "SELECT worker_id, COUNT(*) FROM job_attempts GROUP BY worker_id"
            ).fetchall()
        assert jobs is not None and jobs[0] == 100
        assert attempts is not None and attempts[0] == 100
        assert distinct is not None and distinct[0] == 100
        assert max_attempt is not None and max_attempt[0] == 1
        distribution = {str(row[0]): int(row[1]) for row in dist_rows}
        assert len(distribution) > 1, distribution
        print(
            f"100JOB pids={pids} worker_ids={worker_ids} "
            f"consumers={names} distribution={distribution}"
        )
    finally:
        cluster.stop_all()


def test_three_sleep_jobs_run_concurrently(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_publisher()
        cluster.start_workers(3)
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            started = time.perf_counter()
            job_ids = []
            for _ in range(3):
                response = client.post(
                    "/jobs",
                    json={"job_type": "sleep", "payload": {"seconds": 1.5}},
                )
                assert response.status_code == 202
                job_ids.append(response.json()["id"])

            saw_parallel = False
            snapshot: list[dict[str, object]] = []
            deadline = time.monotonic() + 8.0
            while time.monotonic() < deadline:
                snapshot = [client.get(f"/jobs/{job_id}").json() for job_id in job_ids]
                running = [
                    item for item in snapshot if item["status"] == "RUNNING" and item["worker_id"]
                ]
                running_workers = {item["worker_id"] for item in running}
                if len(running) >= 2 and len(running_workers) >= 2:
                    saw_parallel = True
                    print(f"RUNNING snapshot={running}")
                    break
                time.sleep(0.05)
            assert saw_parallel, snapshot
            bodies = _wait_jobs(client, job_ids, timeout=20.0)
            elapsed = time.perf_counter() - started
            print(f"SLEEP_ELAPSED_SEC={elapsed:.3f}")
            assert elapsed < 4.0
            assert all(item["status"] == "SUCCEEDED" for item in bodies)
            assert all(item["worker_id"] is None for item in bodies)
            assert all(item["result"] == {"slept_seconds": 1.5} for item in bodies)
    finally:
        cluster.stop_all()


def test_one_worker_sleep_jobs_run_sequentially(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_publisher()
        cluster.start_workers(1)
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            started = time.perf_counter()
            job_ids = []
            for _ in range(3):
                response = client.post(
                    "/jobs",
                    json={"job_type": "sleep", "payload": {"seconds": 1.5}},
                )
                assert response.status_code == 202
                job_ids.append(response.json()["id"])
            bodies = _wait_jobs(client, job_ids, timeout=20.0)
            elapsed = time.perf_counter() - started
            print(f"ONE_WORKER_SLEEP_ELAPSED_SEC={elapsed:.3f}")
            assert elapsed >= 4.0
            assert all(item["status"] == "SUCCEEDED" for item in bodies)
    finally:
        cluster.stop_all()


def test_simulate_failure_stays_terminal_with_several_workers(
    redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_publisher()
        cluster.start_workers(3)
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            job_ids = []
            for _ in range(6):
                response = client.post(
                    "/jobs",
                    json={
                        "job_type": "simulate_failure",
                        "payload": {},
                        "max_attempts": 5,
                    },
                )
                assert response.status_code == 202
                job_ids.append(response.json()["id"])
            bodies = _wait_jobs(client, job_ids, terminal="FAILED", timeout=20.0)
            assert all(item["attempt_count"] == 1 for item in bodies)
            assert all(item["error"]["code"] == "SIMULATED_FAILURE" for item in bodies)
            assert all(item["status"] != "RETRYING" for item in bodies)
        with _pg() as conn:
            extra = conn.execute(
                "SELECT COUNT(*) FROM job_attempts WHERE attempt_number > 1"
            ).fetchone()
        assert extra is not None
        assert extra[0] == 0
    finally:
        cluster.stop_all()


def test_graceful_sigterm_finishes_current_sleep_job(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_publisher()
        workers = cluster.start_workers(1)
        worker = workers[0]
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            response = client.post(
                "/jobs",
                json={"job_type": "sleep", "payload": {"seconds": 0.8}},
            )
            job_id = response.json()["id"]
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                body = client.get(f"/jobs/{job_id}").json()
                if body["status"] == "RUNNING":
                    break
                time.sleep(0.05)
            else:
                raise AssertionError("job never reached RUNNING")
            stop_process(worker.proc)
            bodies = _wait_jobs(client, [job_id], timeout=10.0)
            assert bodies[0]["status"] == "SUCCEEDED"
            assert worker.proc.poll() is not None
            logs = worker.log_path.read_text(encoding="utf-8")
            assert "event=worker_stopping" in logs or "event=worker_stopped" in logs
    finally:
        cluster.stop_all()
