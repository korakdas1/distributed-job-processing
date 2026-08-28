"""Prometheus metrics, JSON summary, request IDs, and heartbeat hardening."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import psycopg
import pytest
import redis as redis_sync
from httpx import ASGITransport, AsyncClient

from job_platform.api.app import create_app
from job_platform.core.config import get_settings
from job_platform.db.session import dispose_engine
from job_platform.queue.client import dispose_redis
from job_platform.queue.streams import ensure_consumer_groups
from job_platform.worker.heartbeat import heartbeat_key
from tests.integration.helpers import drain_outbox, process_next_message
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


def _insert_worker(*, worker_id: str, stopped: bool = False) -> None:
    now = datetime.now(UTC)
    with _pg() as conn:
        conn.execute(
            """
            INSERT INTO workers (id, started_at, last_seen_at, stopped_at, hostname, pid)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                worker_id,
                now - timedelta(seconds=5),
                now,
                now if stopped else None,
                "testhost",
                4242,
            ),
        )
        conn.commit()


def _sync_redis() -> redis_sync.Redis:
    settings = get_settings()
    return redis_sync.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
    )


def sample_value(text: str, name: str, **labels: str) -> str:
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        if labels:
            if not line.startswith(name + "{"):
                continue
            if all(f'{key}="{value}"' in line for key, value in labels.items()):
                return line.rsplit(" ", 1)[-1]
        elif line.startswith(name + " "):
            return line.split(" ", 1)[1]
    raise AssertionError(f"missing metric {name} {labels}\n{text[-2000:]}")


def sample_float(text: str, name: str, **labels: str) -> float:
    raw = sample_value(text, name, **labels)
    if raw in {"NaN", "nan", "+Inf", "-Inf"}:
        return float(raw)
    return float(raw)


async def test_metrics_endpoint_exists(client: AsyncClient) -> None:
    response = await client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    body = response.text
    assert "job_platform_dependency_up" in body
    assert "job_platform_http_requests_total" in body
    assert "# HELP job_platform_dependency_up" in body
    assert "# TYPE job_platform_dependency_up gauge" in body
    assert sample_float(body, "job_platform_dependency_up", dependency="postgres") == 1
    assert sample_float(body, "job_platform_dependency_up", dependency="redis") == 1
    print("METRICS_ENDPOINT ok")


async def test_empty_system_metrics(client: AsyncClient) -> None:
    body = (await client.get("/metrics")).text
    for status in (
        "SCHEDULED",
        "QUEUED",
        "RUNNING",
        "RETRYING",
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
    ):
        assert sample_float(body, "job_platform_jobs", status=status) == 0
    for priority in ("CRITICAL", "HIGH", "NORMAL", "LOW"):
        assert sample_float(body, "job_platform_jobs_by_priority", priority=priority) == 0
    assert sample_float(body, "job_platform_jobs_total") == 0
    assert sample_float(body, "job_platform_outbox_unpublished") == 0
    assert sample_float(body, "job_platform_outbox_oldest_unpublished_age_seconds") == 0
    assert sample_float(body, "job_platform_delayed_jobs") == 0
    assert sample_float(body, "job_platform_delayed_due") == 0
    assert sample_float(body, "job_platform_dead_letter_stream_length") == 0
    for status in ("ACTIVE", "STOPPED", "EXPIRED", "UNKNOWN"):
        assert sample_float(body, "job_platform_workers", status=status) == 0
    assert sample_float(body, "job_platform_workers_total") == 0


async def test_metrics_summary_healthy_empty(client: AsyncClient) -> None:
    response = await client.get("/metrics/summary")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "HEALTHY"
    assert body["dependencies"]["postgres"]["available"] is True
    assert body["dependencies"]["redis"]["available"] is True
    assert body["jobs"]["total"] == 0
    assert body["jobs"]["by_status"]["QUEUED"] == 0
    assert body["jobs"]["by_priority"]["LOW"] == 0
    assert body["outbox"]["unpublished"] == 0
    assert body["queues"]["delayed"]["count"] == 0
    assert body["workers"]["total_history"] == 0


async def test_job_state_and_priority_counts(client: AsyncClient) -> None:
    queued = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "queued"}, "priority": "HIGH"},
    )
    scheduled = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "later"},
            "priority": "LOW",
            "delay_seconds": 30,
        },
    )
    success = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "ok"}, "priority": "NORMAL"},
    )
    failed = await client.post(
        "/jobs",
        json={"job_type": "simulate_failure", "payload": {}, "priority": "CRITICAL"},
    )
    assert queued.status_code == 202
    assert scheduled.json()["status"] == "SCHEDULED"
    await ensure_consumer_groups()
    await drain_outbox()
    for _ in range(6):
        await process_next_message()
    leftover = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "still-queued"}, "priority": "HIGH"},
    )
    assert leftover.status_code == 202
    body = (await client.get("/metrics")).text
    assert sample_float(body, "job_platform_jobs", status="QUEUED") >= 1
    assert sample_float(body, "job_platform_jobs", status="SCHEDULED") == 1
    assert sample_float(body, "job_platform_jobs", status="SUCCEEDED") >= 1
    assert sample_float(body, "job_platform_jobs", status="FAILED") >= 1
    assert sample_float(body, "job_platform_jobs_by_priority", priority="HIGH") >= 1
    assert sample_float(body, "job_platform_jobs_by_priority", priority="LOW") >= 1
    assert sample_float(body, "job_platform_jobs_by_priority", priority="NORMAL") >= 1
    assert sample_float(body, "job_platform_jobs_by_priority", priority="CRITICAL") >= 1
    assert sample_float(body, "job_platform_jobs_total") == 5
    assert success.json()["id"]
    assert failed.json()["id"]
    print("JOB_COUNTS queued+scheduled+succeeded+failed")


async def test_stream_length_metrics(client: AsyncClient) -> None:
    for priority in ("CRITICAL", "HIGH", "NORMAL", "LOW"):
        created = await client.post(
            "/jobs",
            json={"job_type": "word_count", "payload": {"text": priority}, "priority": priority},
        )
        assert created.status_code == 202
    await drain_outbox()
    body = (await client.get("/metrics")).text
    for label in ("critical", "high", "normal", "low"):
        assert sample_float(body, "job_platform_redis_stream_length", stream=label) >= 1
    print("STREAM_LENGTHS ok")


async def test_delayed_metrics_before_due(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "wait"},
            "delay_seconds": 30,
        },
    )
    assert created.status_code == 202
    await drain_outbox()
    body = (await client.get("/metrics")).text
    assert sample_float(body, "job_platform_delayed_jobs") >= 1
    assert sample_float(body, "job_platform_delayed_due") == 0
    print("DELAYED count>=1 due=0")


async def test_delayed_due_positive_before_scheduler(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "already due"},
            "delay_seconds": 30,
        },
    )
    job_id = created.json()["id"]
    await drain_outbox()
    settings = get_settings()
    client_redis = _sync_redis()
    try:
        client_redis.zadd(settings.redis_delayed_zset, {job_id: time.time() - 5})
    finally:
        client_redis.close()
    body = (await client.get("/metrics")).text
    due = sample_float(body, "job_platform_delayed_due")
    assert due >= 1
    print(f"DELAYED_DUE {due}")


async def test_dead_letter_metric(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "simulate_failure", "payload": {}},
    )
    assert created.status_code == 202
    await ensure_consumer_groups()
    await drain_outbox()
    await process_next_message()
    await drain_outbox()
    body = (await client.get("/metrics")).text
    assert sample_float(body, "job_platform_dead_letter_stream_length") >= 1
    print("DLQ_LENGTH", sample_value(body, "job_platform_dead_letter_stream_length"))


async def test_metrics_do_not_mutate_state(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "stable"}},
    )
    job_id = created.json()["id"]
    first = (await client.get("/metrics")).text
    second = (await client.get("/metrics")).text
    assert sample_float(first, "job_platform_jobs_total") == sample_float(
        second, "job_platform_jobs_total"
    )
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.status_code == 200
    assert fetched.json()["status"] == "QUEUED"
    with _pg() as conn:
        published = conn.execute(
            "SELECT published_at FROM outbox_events WHERE job_id = %s",
            (job_id,),
        ).fetchone()
    assert published is not None
    assert published[0] is None


async def test_concurrent_metrics_scrapes(client: AsyncClient) -> None:
    responses = await asyncio.gather(*[client.get("/metrics") for _ in range(5)])
    assert all(item.status_code == 200 for item in responses)
    assert all("job_platform_dependency_up" in item.text for item in responses)


async def test_http_request_metrics_use_route_templates(client: AsyncClient) -> None:
    health = await client.get("/health")
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "metric-route"}},
    )
    job_id = created.json()["id"]
    fetched = await client.get(f"/jobs/{job_id}")
    missing = await client.get("/definitely-not-a-route")
    assert health.status_code == 200
    assert fetched.status_code == 200
    assert missing.status_code == 404
    body = (await client.get("/metrics")).text
    assert (
        sample_float(
            body,
            "job_platform_http_requests_total",
            method="GET",
            route="/health",
            status_code="200",
        )
        >= 1
    )
    assert (
        sample_float(
            body,
            "job_platform_http_requests_total",
            method="GET",
            route="/jobs/{job_id}",
            status_code="200",
        )
        >= 1
    )
    assert (
        sample_float(
            body,
            "job_platform_http_requests_total",
            method="POST",
            route="/jobs",
            status_code="202",
        )
        >= 1
    )
    assert (
        sample_float(
            body,
            "job_platform_http_requests_total",
            method="GET",
            route="unmatched",
            status_code="404",
        )
        >= 1
    )
    for line in body.splitlines():
        if "route=" in line:
            assert job_id not in line
    assert "job_platform_http_request_duration_seconds_bucket" in body
    assert "job_platform_http_request_duration_seconds_count" in body
    assert "job_platform_http_request_duration_seconds_sum" in body
    print("HTTP_ROUTE_TEMPLATE /jobs/{job_id}")


async def test_request_id_header_is_unique(client: AsyncClient) -> None:
    first = await client.get("/health")
    second = await client.get("/health")
    left = first.headers.get("x-request-id")
    right = second.headers.get("x-request-id")
    assert left
    assert right
    assert left != right
    print("REQUEST_IDS", left, right)


async def test_outbox_backlog_when_redis_down(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = str(get_settings().redis_port)
    monkeypatch.setenv("REDIS_PORT", "1")
    get_settings.cache_clear()
    await dispose_redis()
    posted = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "buffered-metrics"}},
    )
    assert posted.status_code == 202
    metrics = await client.get("/metrics")
    assert metrics.status_code == 200
    body = metrics.text
    assert sample_float(body, "job_platform_dependency_up", dependency="postgres") == 1
    assert sample_float(body, "job_platform_dependency_up", dependency="redis") == 0
    unpublished = sample_float(body, "job_platform_outbox_unpublished")
    age = sample_float(body, "job_platform_outbox_oldest_unpublished_age_seconds")
    assert unpublished >= 1
    assert age >= 0
    assert sample_value(body, "job_platform_delayed_jobs") in {"NaN", "nan"}
    summary = await client.get("/metrics/summary")
    assert summary.status_code == 200
    payload = summary.json()
    assert payload["status"] == "DEGRADED"
    assert payload["dependencies"]["postgres"]["available"] is True
    assert payload["dependencies"]["redis"]["available"] is False
    assert payload["outbox"]["unpublished"] >= 1
    assert payload["queues"] is None
    ready = await client.get("/ready")
    assert ready.status_code == 200
    ready_body = ready.json()
    assert ready_body["status"] == "ready"
    assert ready_body["degraded"] is True
    assert ready_body["dependencies"]["postgres"] == "up"
    assert ready_body["dependencies"]["redis"] == "down"
    health = await client.get("/health")
    assert health.status_code == 200
    print(f"OUTBOX_REDIS_DOWN unpublished={unpublished} age={age}")
    monkeypatch.setenv("REDIS_PORT", original)
    get_settings.cache_clear()
    await dispose_redis()


async def test_outbox_age_does_not_decrease(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = str(get_settings().redis_port)
    monkeypatch.setenv("REDIS_PORT", "1")
    get_settings.cache_clear()
    await dispose_redis()
    posted = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "age"}},
    )
    assert posted.status_code == 202
    metrics_text = (await client.get("/metrics")).text
    first = sample_float(metrics_text, "job_platform_outbox_oldest_unpublished_age_seconds")
    await asyncio.sleep(0.3)
    metrics_text = (await client.get("/metrics")).text
    second = sample_float(metrics_text, "job_platform_outbox_oldest_unpublished_age_seconds")
    assert second + 0.05 >= first
    monkeypatch.setenv("REDIS_PORT", original)
    get_settings.cache_clear()
    await dispose_redis()


async def test_metrics_postgres_down_uses_nan(monkeypatch: pytest.MonkeyPatch) -> None:
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
        metrics = await isolated.get("/metrics")
        assert metrics.status_code == 200
        body = metrics.text
        assert sample_float(body, "job_platform_dependency_up", dependency="postgres") == 0
        assert sample_float(body, "job_platform_dependency_up", dependency="redis") == 1
        jobs_total = sample_value(body, "job_platform_jobs_total")
        assert jobs_total in {"NaN", "nan"}
        summary = await isolated.get("/metrics/summary")
        assert summary.status_code == 200
        payload = summary.json()
        assert payload["status"] == "UNAVAILABLE"
        assert payload["dependencies"]["postgres"]["available"] is False
        assert payload["jobs"] is None
        assert payload["queues"] is not None
        print("POSTGRES_DOWN metrics=200 summary=UNAVAILABLE")
    await dispose_engine()
    get_settings.cache_clear()
    await dispose_redis()


async def test_nonexpiring_heartbeat_is_unknown(client: AsyncClient) -> None:
    _insert_worker(worker_id="worker-nottl001")
    redis = _sync_redis()
    try:
        redis.set(
            heartbeat_key("worker-nottl001"),
            '{"worker_id":"worker-nottl001","heartbeat_at":"2026-08-26T00:00:00+00:00"}',
        )
        assert redis.pttl(heartbeat_key("worker-nottl001")) == -1
    finally:
        redis.close()
    listed = await client.get("/workers")
    assert listed.status_code == 200
    item = listed.json()["items"][0]
    assert item["status"] == "UNKNOWN"
    assert item["is_alive"] is None
    assert item["heartbeat_ttl_ms"] is None
    body = (await client.get("/metrics")).text
    assert sample_float(body, "job_platform_workers", status="UNKNOWN") >= 1
    assert sample_float(body, "job_platform_workers", status="ACTIVE") == 0
    print("PTTL_MINUS_ONE UNKNOWN")


async def test_heartbeat_identity_mismatch_is_unknown(client: AsyncClient) -> None:
    _insert_worker(worker_id="worker-mismatch1")
    redis = _sync_redis()
    try:
        redis.set(
            heartbeat_key("worker-mismatch1"),
            '{"worker_id":"worker-other000","heartbeat_at":"2026-08-26T00:00:00+00:00"}',
            px=5000,
        )
    finally:
        redis.close()
    listed = await client.get("/workers")
    assert listed.status_code == 200
    item = listed.json()["items"][0]
    assert item["status"] == "UNKNOWN"
    assert item["is_alive"] is None
    body = (await client.get("/metrics")).text
    assert sample_float(body, "job_platform_workers", status="UNKNOWN") >= 1
    assert sample_float(body, "job_platform_workers", status="ACTIVE") == 0


def test_worker_and_pending_metrics(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_publisher()
        cluster.start_workers(2)
        with httpx.Client(base_url=cluster.base_url, timeout=15.0) as http:
            created = http.post(
                "/jobs",
                json={
                    "job_type": "sleep",
                    "payload": {"seconds": 2},
                    "priority": "HIGH",
                },
            )
            assert created.status_code == 202
            deadline = time.monotonic() + 8.0
            while time.monotonic() < deadline:
                job = http.get(f"/jobs/{created.json()['id']}").json()
                if job["status"] == "RUNNING":
                    break
                time.sleep(0.05)
            else:
                raise AssertionError("sleep job never reached RUNNING")
            metrics = http.get("/metrics")
            assert metrics.status_code == 200
            body = metrics.text
            active = sample_float(body, "job_platform_workers", status="ACTIVE")
            pending = sample_float(body, "job_platform_redis_pending", stream="high")
            assert active >= 2
            assert pending >= 1
            print(f"WORKERS_ACTIVE {active} HIGH_PENDING {pending}")
            cluster.stop_worker_gracefully(0)
            deadline = time.monotonic() + 8.0
            stopped = 0.0
            while time.monotonic() < deadline:
                body = http.get("/metrics").text
                stopped = sample_float(body, "job_platform_workers", status="STOPPED")
                if stopped >= 1:
                    break
                time.sleep(0.1)
            assert stopped >= 1
            print("WORKERS_STOPPED", stopped)
    finally:
        cluster.stop_all()


def test_expired_worker_metric(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_workers(1)
        with httpx.Client(base_url=cluster.base_url, timeout=15.0) as http:
            body = http.get("/metrics").text
            assert sample_float(body, "job_platform_workers", status="ACTIVE") >= 1
            cluster.kill_worker_sigkill(0)
            time.sleep(1.3)
            body = http.get("/metrics").text
            expired = sample_float(body, "job_platform_workers", status="EXPIRED")
            assert expired >= 1
            print("WORKERS_EXPIRED", expired)
    finally:
        cluster.stop_all()
