"""Real Redis loss is opt-in and guarded; workers are never restarted to recover."""

from __future__ import annotations

import asyncio
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
from httpx import AsyncClient
from redis.exceptions import ResponseError
from sqlalchemy import select

from job_platform.core.config import get_settings
from job_platform.db.session import get_session_factory
from job_platform.models.job import Job, JobAttempt
from job_platform.models.outbox import OutboxEvent
from job_platform.queue.client import get_redis
from job_platform.queue.streams import autoclaim_stale, read_one_from_stream
from job_platform.worker.runtime import is_nogroup_error
from tests.disposable_infrastructure import ROOT, DisposableInfrastructure
from tests.integration.helpers import drain_outbox, process_next_message
from tests.integration.process_harness import ManagedProcess, ProcessCluster, wait_log_contains

pytestmark = [pytest.mark.integration, pytest.mark.disruptive]


def start_worker(cluster: ProcessCluster, index: int) -> ManagedProcess:
    log_path = cluster.tmp_path / f"worker-{index}.log"
    handle = log_path.open("w", buffering=1)
    # A long lease separates explicit group repair/redelivery from ordinary lease expiry.
    cluster.env["JOB_LEASE_TIMEOUT_SECONDS"] = "300"
    cluster.env["WORKER_RECLAIM_INTERVAL_MS"] = "100"
    proc = subprocess.Popen(
        [
            sys.executable,
            str(ROOT / "tests/integration/group_recovery_worker.py"),
            str(cluster.tmp_path),
            str(index),
        ],
        cwd=ROOT,
        env=cluster.env,
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    worker = ManagedProcess(proc, log_path, handle)
    cluster._workers.append(worker)
    wait_log_contains(log_path, "event=worker_ready")
    return worker


async def wait_file(path: Path) -> None:
    async with asyncio.timeout(10):
        while not path.exists():
            await asyncio.sleep(0.01)


async def wait_status(client: AsyncClient, job_id: str, status: str) -> dict[str, object]:
    async with asyncio.timeout(10):
        while True:
            body = (await client.get(f"/jobs/{job_id}")).json()
            if body["status"] == status:
                return body
            await asyncio.sleep(0.02)


async def submit(client: AsyncClient, **kwargs: object) -> str:
    response = await client.post(
        "/jobs", json={"job_type": "word_count", "payload": {"text": "retained work"}, **kwargs}
    )
    assert response.status_code == 202
    await drain_outbox()
    return str(response.json()["id"])


async def attempts(job_id: str) -> list[JobAttempt]:
    async with get_session_factory()() as session:
        return list(
            await session.scalars(
                select(JobAttempt)
                .where(JobAttempt.job_id == uuid.UUID(job_id))
                .order_by(JobAttempt.attempt_number)
            )
        )


async def lose_group(
    infrastructure: DisposableInfrastructure, *, delete_stream: bool = False
) -> None:
    # Recheck the complete ownership contract immediately before every destructive command.
    settings = get_settings()
    infrastructure.check_targets(settings)
    infrastructure.verify()
    redis = get_redis()
    if delete_stream:
        assert await redis.delete(settings.redis_stream_normal) == 1
    else:
        assert await redis.xgroup_destroy(
            settings.redis_stream_normal, settings.redis_consumer_group
        )


async def assert_group_exists() -> None:
    settings = get_settings()
    groups = await get_redis().xinfo_groups(settings.redis_stream_normal)
    assert settings.redis_consumer_group in [group["name"] for group in groups]


def release_all(path: Path) -> None:
    # Always unblock test instrumentation before requesting production graceful shutdown.
    for name in ("poll-0.release", "poll-1.release", "repair.release", "handler.release"):
        (path / name).touch()


async def test_two_workers_repair_retained_work_with_same_pids(
    client: AsyncClient, disposable_infrastructure: DisposableInfrastructure, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        workers = [start_worker(cluster, index) for index in range(2)]
        pids = cluster.worker_pids()
        job_id = await submit(client)
        entries = await get_redis().xrange("jobs:normal")
        assert len(entries) == 1
        message_id = entries[0][0]
        await lose_group(disposable_infrastructure)
        assert await get_redis().xrange("jobs:normal") == entries
        # Check redis-py's actual wire-error format for both runtime entry points.
        with pytest.raises(ResponseError) as read_error:
            await read_one_from_stream("jobs:normal", "format-probe", block_ms=0)
        with pytest.raises(ResponseError) as reclaim_error:
            await autoclaim_stale("jobs:normal", "format-probe", min_idle_ms=300_000)
        assert is_nogroup_error(read_error.value)
        assert is_nogroup_error(reclaim_error.value)
        assert await get_redis().xack("jobs:normal", "job-workers", message_id) == 0
        for index in range(2):
            (tmp_path / f"poll-{index}.release").touch()
        # Both have observed NOGROUP before either is permitted to recreate the group.
        await asyncio.gather(*(wait_file(tmp_path / f"repair-{i}.entered") for i in range(2)))
        assert await attempts(job_id) == []
        (tmp_path / "repair.release").touch()
        body = await wait_status(client, job_id, "SUCCEEDED")
        for worker in workers:
            wait_log_contains(worker.log_path, "event=consumer_group_recovery_succeeded")
            assert worker.proc.poll() is None
        assert cluster.worker_pids() == pids
        assert body["attempt_count"] == 1
        assert body["result"] == {"word_count": 2}
        (attempt,) = await attempts(job_id)
        assert (attempt.delivery_stream, attempt.delivery_message_id) == ("jobs:normal", message_id)
        await assert_group_exists()
        print(f"GROUP_REPAIR_SAME_PIDS pids={pids} job={job_id} message={message_id}")
    finally:
        release_all(tmp_path)
        cluster.stop_all()


@pytest.mark.parametrize("peer_repairs", [True, False])
async def test_live_handler_retains_ownership_during_group_loss(
    client: AsyncClient,
    disposable_infrastructure: DisposableInfrastructure,
    tmp_path: Path,
    peer_repairs: bool,
) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        original = start_worker(cluster, 0)
        if peer_repairs:
            start_worker(cluster, 1)
        pids = cluster.worker_pids()
        (tmp_path / "poll-0.release").touch()
        (tmp_path / "repair.release").touch()
        job_id = await submit(client, payload={"text": "blocked handler"})
        await wait_file(tmp_path / "handler-0.entered")
        before = await wait_status(client, job_id, "RUNNING")
        (active,) = await attempts(job_id)
        await lose_group(disposable_infrastructure)
        if peer_repairs:
            (tmp_path / "poll-1.release").touch()
            await wait_file(tmp_path / f"processed-1-{job_id}")
            await assert_group_exists()
            # Redelivery is pending, not a second execution; the new PEL has its own idle clock.
            pending = await get_redis().xpending("jobs:normal", "job-workers")
            assert pending["pending"] == 1
            assert await autoclaim_stale("jobs:normal", "lease-probe", min_idle_ms=300_000) is None
        else:
            assert await get_redis().xinfo_groups("jobs:normal") == []
        still = (await client.get(f"/jobs/{job_id}")).json()
        assert still["status"] == "RUNNING"
        assert still["attempt_count"] == 1
        assert still["worker_id"] == before["worker_id"]
        (unchanged,) = await attempts(job_id)
        assert unchanged.id == active.id
        assert unchanged.status == "RUNNING"
        assert unchanged.delivery_message_id == active.delivery_message_id
        assert not (tmp_path / "handler-1.entered").exists()
        (tmp_path / "handler.release").touch()
        finished = await wait_status(client, job_id, "SUCCEEDED")
        await wait_file(tmp_path / f"processed-0-{job_id}")
        if not peer_repairs:
            # Redis XACK on the vanished group returns zero. The next read/reclaim repairs it.
            wait_log_contains(original.log_path, "TEST_XACK_RESULT=0")
            wait_log_contains(original.log_path, "event=consumer_group_recovery_succeeded")
        await assert_group_exists()
        assert finished["result"] == {"word_count": 2}
        assert finished["attempt_count"] == 1
        (completed,) = await attempts(job_id)
        assert completed.id == active.id and completed.status == "SUCCEEDED"
        assert cluster.worker_pids() == pids
        assert all(cluster.worker_alive(i) for i in range(len(pids)))
        print(f"LIVE_GROUP_LOSS peer={peer_repairs} pids={pids} attempt={active.id}")
    finally:
        release_all(tmp_path)
        cluster.stop_all()


async def test_terminal_retained_entries_do_not_execute_again(
    client: AsyncClient, disposable_infrastructure: DisposableInfrastructure, tmp_path: Path
) -> None:
    succeeded = await submit(client)
    await process_next_message()
    failed = await submit(client, job_type="simulate_failure", payload={})
    await process_next_message()
    cancelled = await submit(client)
    assert (await client.delete(f"/jobs/{cancelled}")).status_code == 200
    expected = {succeeded: ("SUCCEEDED", 1), failed: ("FAILED", 1), cancelled: ("CANCELLED", 0)}
    cluster = ProcessCluster(tmp_path)
    try:
        worker = start_worker(cluster, 0)
        pid = worker.proc.pid
        await lose_group(disposable_infrastructure)
        release_all(tmp_path)
        for job_id, (status, count) in expected.items():
            await wait_file(tmp_path / f"processed-0-{job_id}")
            body = (await client.get(f"/jobs/{job_id}")).json()
            assert (body["status"], body["attempt_count"]) == (status, count)
            assert len(await attempts(job_id)) == count
        assert (await get_redis().xpending("jobs:normal", "job-workers"))["pending"] == 0
        assert worker.proc.pid == pid and worker.proc.poll() is None
    finally:
        release_all(tmp_path)
        cluster.stop_all()


async def test_deleted_delivery_stays_stranded_but_new_traffic_executes(
    client: AsyncClient, disposable_infrastructure: DisposableInfrastructure, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        worker = start_worker(cluster, 0)
        pid = worker.proc.pid
        old = await submit(client)
        async with get_session_factory()() as session:
            event = await session.scalar(
                select(OutboxEvent).where(OutboxEvent.job_id == uuid.UUID(old))
            )
            assert event is not None and event.published_at is not None
            old_message = event.redis_message_id
            old_publication = event.published_at
        await lose_group(disposable_infrastructure, delete_stream=True)
        new = await submit(client)
        release_all(tmp_path)
        assert (await wait_status(client, new, "SUCCEEDED"))["attempt_count"] == 1
        await assert_group_exists()
        assert worker.proc.pid == pid and worker.proc.poll() is None
        async with get_session_factory()() as session:
            job = await session.get(Job, uuid.UUID(old))
            event = await session.scalar(
                select(OutboxEvent).where(OutboxEvent.job_id == uuid.UUID(old))
            )
            assert job is not None and (job.status, job.attempt_count) == ("QUEUED", 0)
            assert event is not None and event.published_at == old_publication
            assert event.redis_message_id == old_message
        assert await attempts(old) == []
        assert all(fields["job_id"] != old for _, fields in await get_redis().xrange("jobs:normal"))
        print(f"DELETED_DELIVERY_NOT_RECOVERED pid={pid} old={old} new={new}")
    finally:
        release_all(tmp_path)
        cluster.stop_all()


@pytest.mark.parametrize("stop_during_failure", [False, True])
async def test_repair_failure_retries_or_stops_without_restart(
    client: AsyncClient,
    disposable_infrastructure: DisposableInfrastructure,
    tmp_path: Path,
    stop_during_failure: bool,
) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        worker = start_worker(cluster, 0)
        pid = worker.proc.pid
        job_id = await submit(client)
        (tmp_path / "repair.fail").touch()
        await lose_group(disposable_infrastructure)
        release_all(tmp_path)
        wait_log_contains(worker.log_path, "event=consumer_group_recovery_failed")
        await asyncio.sleep(1.2)
        failures = worker.log_path.read_text().count("event=consumer_group_recovery_failed")
        assert 2 <= failures <= 3  # One-second pause, not a tight retry loop.
        assert worker.proc.poll() is None and worker.proc.pid == pid
        assert await attempts(job_id) == []
        if stop_during_failure:
            start = time.monotonic()
            cluster.stop_worker_gracefully(timeout=3)
            assert worker.proc.returncode == 0
            assert time.monotonic() - start < 3
        else:
            (tmp_path / "repair.fail").unlink()
            assert (await wait_status(client, job_id, "SUCCEEDED"))["attempt_count"] == 1
            assert worker.proc.poll() is None and worker.proc.pid == pid
    finally:
        release_all(tmp_path)
        cluster.stop_all()
