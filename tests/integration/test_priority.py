"""Priority routing, consumer groups, retry redispatch, and fairness."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from pathlib import Path

import httpx
import psycopg
import pytest
import redis as redis_sync
from httpx import AsyncClient

from job_platform.core.config import get_settings
from job_platform.core.enums import JobPriority
from job_platform.queue.priority import ready_streams, stream_for_priority
from job_platform.queue.streams import publish_job_id
from job_platform.scheduler.promoter import promote_due_jobs
from job_platform.tasks.errors import RetryableTaskError
from job_platform.tasks.handlers import execute_handler
from tests.integration.helpers import (
    drain_outbox,
    process_next_message,
    stream_entries,
)
from tests.integration.process_harness import ProcessCluster

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


def _sync_redis() -> redis_sync.Redis:
    settings = get_settings()
    return redis_sync.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
        socket_connect_timeout=2,
    )


def _group_names(stream: str) -> list[str]:
    client = _sync_redis()
    try:
        info = client.xinfo_groups(stream)
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


async def test_each_priority_publishes_to_its_ready_stream(client: AsyncClient) -> None:
    created: dict[str, str] = {}
    for priority in ("CRITICAL", "HIGH", "NORMAL", "LOW"):
        response = await client.post(
            "/jobs",
            json={
                "job_type": "word_count",
                "payload": {"text": priority.lower()},
                "priority": priority,
            },
        )
        assert response.status_code == 202
        created[priority] = response.json()["id"]
    await drain_outbox()
    for priority, job_id in created.items():
        stream = stream_for_priority(priority)
        entries = await stream_entries(stream)
        assert [fields["job_id"] for _mid, fields in entries] == [job_id]
        assert "payload" not in entries[0][1]


async def test_empty_payload_dispatch_still_routes_by_postgres_priority(
    client: AsyncClient,
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "old payload"}, "priority": "LOW"},
    )
    job_id = created.json()["id"]
    with _pg() as conn:
        conn.execute(
            "UPDATE outbox_events SET payload = '{}'::jsonb WHERE job_id = %s",
            (job_id,),
        )
        conn.commit()
    await drain_outbox()
    entries = await stream_entries(get_settings().redis_stream_low)
    assert entries[0][1]["job_id"] == job_id
    assert await stream_entries(get_settings().redis_stream_normal) == []


async def test_retryable_high_job_redispatches_to_jobs_high(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}
    real = execute_handler

    async def flaky(job_type: object, payload: object, context: object = None) -> dict[str, object]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RetryableTaskError("SIMULATED_RETRYABLE_FAILURE", "first attempt")
        return await real(job_type, payload, context)  # type: ignore[arg-type]

    monkeypatch.setattr("job_platform.worker.processor.execute_handler", flaky)
    created = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "high retry"},
            "priority": "HIGH",
            "max_attempts": 3,
        },
    )
    job_id = created.json()["id"]
    await drain_outbox()
    high_entries = await stream_entries(get_settings().redis_stream_high)
    assert [fields["job_id"] for _mid, fields in high_entries] == [job_id]
    assert await stream_entries(get_settings().redis_stream_normal) == []

    await process_next_message()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "RETRYING"
    await drain_outbox()
    remaining = datetime.fromisoformat(body["next_retry_at"]).timestamp() - time.time()
    if remaining > 0:
        await asyncio.sleep(remaining + 0.05)
    await promote_due_jobs()
    await drain_outbox()
    high_after = await stream_entries(get_settings().redis_stream_high)
    assert any(fields["job_id"] == job_id for _mid, fields in high_after)
    assert await stream_entries(get_settings().redis_stream_normal) == []
    await process_next_message()
    done = (await client.get(f"/jobs/{job_id}")).json()
    assert done["status"] == "SUCCEEDED"
    assert done["attempt_count"] == 2
    with _pg() as conn:
        streams = conn.execute(
            "SELECT delivery_stream FROM job_attempts WHERE job_id = %s ORDER BY attempt_number",
            (job_id,),
        ).fetchall()
    assert [row[0] for row in streams] == [
        get_settings().redis_stream_high,
        get_settings().redis_stream_high,
    ]


async def test_cross_stream_duplicate_still_one_logical_attempt(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "cross"}, "priority": "HIGH"},
    )
    job_id = created.json()["id"]
    await drain_outbox()
    await publish_job_id(job_id, stream=get_settings().redis_stream_normal)
    await process_next_message("worker-cross01")
    await process_next_message("worker-cross02")
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "SUCCEEDED"
    assert fetched.json()["attempt_count"] == 1
    with _pg() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM job_attempts WHERE job_id = %s",
            (job_id,),
        ).fetchone()
    assert count is not None
    assert count[0] == 1


def test_worker_creates_groups_on_all_ready_streams_not_dead(
    redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_workers(1)
        settings = get_settings()
        for stream in ready_streams():
            assert settings.redis_consumer_group in _group_names(stream), stream
        assert settings.redis_consumer_group not in _group_names(settings.redis_dead_letter_stream)
    finally:
        cluster.stop_all()


def test_mixed_priorities_all_succeed_with_multiple_workers(
    redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_publisher()
        cluster.start_workers(3)
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            job_ids: list[str] = []
            priorities = ["CRITICAL", "HIGH", "NORMAL", "LOW"]
            for index in range(80):
                response = client.post(
                    "/jobs",
                    json={
                        "job_type": "word_count",
                        "payload": {"text": f"mix {index}"},
                        "priority": priorities[index % 4],
                    },
                )
                assert response.status_code == 202
                job_ids.append(response.json()["id"])
            deadline = time.monotonic() + 45.0
            bodies: list[dict[str, object]] = []
            while time.monotonic() < deadline:
                bodies = [client.get(f"/jobs/{job_id}").json() for job_id in job_ids]
                if all(item["status"] == "SUCCEEDED" for item in bodies):
                    break
                time.sleep(0.05)
            assert all(item["status"] == "SUCCEEDED" for item in bodies)
            assert all(item["attempt_count"] == 1 for item in bodies)
        with _pg() as conn:
            dist = conn.execute(
                "SELECT worker_id, COUNT(*) FROM job_attempts GROUP BY worker_id"
            ).fetchall()
            streams = conn.execute(
                "SELECT delivery_stream, COUNT(*) FROM job_attempts GROUP BY delivery_stream"
            ).fetchall()
        assert len(dist) > 1
        stream_counts = {str(row[0]): int(row[1]) for row in streams}
        assert stream_counts[get_settings().redis_stream_critical] == 20
        assert stream_counts[get_settings().redis_stream_high] == 20
        assert stream_counts[get_settings().redis_stream_normal] == 20
        assert stream_counts[get_settings().redis_stream_low] == 20
    finally:
        cluster.stop_all()


def test_low_job_runs_while_critical_backlog_remains(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_publisher()
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            critical_ids: list[str] = []
            for _index in range(24):
                response = client.post(
                    "/jobs",
                    json={
                        "job_type": "sleep",
                        "payload": {"seconds": 0.25},
                        "priority": "CRITICAL",
                    },
                )
                assert response.status_code == 202, response.text
                critical_ids.append(response.json()["id"])
            low = client.post(
                "/jobs",
                json={
                    "job_type": "word_count",
                    "payload": {"text": "starve-check"},
                    "priority": "LOW",
                },
            )
            assert low.status_code == 202
            low_id = low.json()["id"]
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                with _pg() as conn:
                    unpublished = conn.execute(
                        "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL"
                    ).fetchone()
                if unpublished is not None and unpublished[0] == 0:
                    break
                time.sleep(0.05)
            else:
                raise AssertionError("publisher did not drain before worker start")
            cluster.start_workers(1)
            low_done = None
            remaining_critical = None
            deadline = time.monotonic() + 20.0
            while time.monotonic() < deadline:
                low_done = client.get(f"/jobs/{low_id}").json()
                if low_done["status"] in {"SUCCEEDED", "FAILED"}:
                    remaining_critical = [
                        client.get(f"/jobs/{job_id}").json()["status"] for job_id in critical_ids
                    ]
                    break
                time.sleep(0.05)
            assert low_done is not None
            assert low_done["status"] == "SUCCEEDED"
            assert remaining_critical is not None
            unfinished = [
                status for status in remaining_critical if status in {"QUEUED", "RUNNING"}
            ]
            print(
                f"FAIRNESS low={low_done['status']} "
                f"critical_unfinished={len(unfinished)}/{len(critical_ids)}"
            )
            assert unfinished, remaining_critical
    finally:
        cluster.stop_all()


def test_priority_values_are_canonical() -> None:
    assert [item.value for item in JobPriority] == ["CRITICAL", "HIGH", "NORMAL", "LOW"]
    assert ready_streams() == (
        "jobs:critical",
        "jobs:high",
        "jobs:normal",
        "jobs:low",
    )
