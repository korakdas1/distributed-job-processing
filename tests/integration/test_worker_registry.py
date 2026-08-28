"""Worker registry, Redis TTL heartbeats, and GET /workers process tests."""

from __future__ import annotations

import json
import time
from pathlib import Path

import httpx
import psycopg
import pytest
import redis as redis_sync

from job_platform.core.config import get_settings
from job_platform.worker.heartbeat import heartbeat_key
from tests.integration.process_harness import (
    ProcessCluster,
    start_project_postgres,
    start_project_redis,
    stop_project_postgres,
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


def _redis() -> redis_sync.Redis:
    settings = get_settings()
    return redis_sync.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
    )


def _wait_worker(client: httpx.Client, worker_id: str, status: str, timeout: float = 8.0) -> dict:
    deadline = time.monotonic() + timeout
    last: dict | None = None
    while time.monotonic() < deadline:
        body = client.get("/workers").json()
        for item in body["items"]:
            if item["id"] == worker_id:
                last = item
                if item["status"] == status:
                    return item
        time.sleep(0.05)
    raise AssertionError(f"worker {worker_id} never reached {status}: {last}")


def test_worker_registers_heartbeat_and_lists_active(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_workers(1)
        worker_id = cluster.worker_ids()[0]
        pid = cluster.worker_pid(0)
        wait_log_contains(cluster.worker_log_path(0), "event=worker_registered")
        with _pg() as conn:
            row = conn.execute(
                """
                SELECT id, pid, hostname, started_at, last_seen_at, stopped_at
                FROM workers WHERE id = %s
                """,
                (worker_id,),
            ).fetchone()
        assert row is not None
        assert row[0] == worker_id
        assert int(row[1]) == pid
        assert str(row[2])
        assert row[5] is None
        client_redis = _redis()
        try:
            key = heartbeat_key(worker_id)
            raw = client_redis.get(key)
            ttl = int(client_redis.pttl(key))
            assert raw is not None
            payload = json.loads(str(raw))
            assert payload["worker_id"] == worker_id
            assert ttl > 0
            first_at = payload["heartbeat_at"]
            time.sleep(0.35)
            raw2 = client_redis.get(key)
            ttl2 = int(client_redis.pttl(key))
            payload2 = json.loads(str(raw2))
            assert payload2["heartbeat_at"] != first_at or ttl2 > 0
        finally:
            client_redis.close()
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as http:
            item = _wait_worker(http, worker_id, "ACTIVE")
            assert item["is_alive"] is True
            assert item["stopped_at"] is None
            assert item["heartbeat_at"] is not None
            assert item["pid"] == pid
        print(f"ACTIVE worker_id={worker_id} pid={pid}")
    finally:
        cluster.stop_all()


def test_three_workers_have_distinct_registry_rows(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_workers(3)
        ids = cluster.worker_ids()
        pids = [cluster.worker_pid(index) for index in range(3)]
        assert len(set(ids)) == 3
        assert len(set(pids)) == 3
        with _pg() as conn:
            rows = conn.execute("SELECT id, pid FROM workers ORDER BY id").fetchall()
        assert {row[0] for row in rows} == set(ids)
        client_redis = _redis()
        try:
            for worker_id in ids:
                assert client_redis.exists(heartbeat_key(worker_id)) == 1
        finally:
            client_redis.close()
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as http:
            body = http.get("/workers").json()
            assert body["total"] == 3
            assert {item["id"] for item in body["items"]} == set(ids)
            assert all(item["status"] == "ACTIVE" for item in body["items"])
        print(f"MULTI ids={','.join(ids)} pids={','.join(str(p) for p in pids)}")
    finally:
        cluster.stop_all()


def test_heartbeat_continues_during_long_job(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_publisher()
        cluster.start_workers(1)
        worker_id = cluster.worker_ids()[0]
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as http:
            created = http.post(
                "/jobs",
                json={"job_type": "sleep", "payload": {"seconds": 2.5}},
            )
            assert created.status_code == 202
            job_id = created.json()["id"]
            deadline = time.monotonic() + 8.0
            running = None
            while time.monotonic() < deadline:
                body = http.get(f"/jobs/{job_id}").json()
                if body["status"] == "RUNNING":
                    running = body
                    break
                time.sleep(0.05)
            assert running is not None
            time.sleep(1.2)
            still_running = http.get(f"/jobs/{job_id}").json()
            assert still_running["status"] == "RUNNING"
            item = _wait_worker(http, worker_id, "ACTIVE", timeout=2.0)
            assert item["is_alive"] is True
            deadline = time.monotonic() + 8.0
            done = None
            while time.monotonic() < deadline:
                body = http.get(f"/jobs/{job_id}").json()
                if body["status"] == "SUCCEEDED":
                    done = body
                    break
                time.sleep(0.05)
            assert done is not None
            print(f"LONG_JOB worker_id={worker_id} job={job_id}")
    finally:
        cluster.stop_all()


def test_graceful_sigterm_marks_stopped(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_workers(1)
        worker_id = cluster.worker_ids()[0]
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as http:
            _wait_worker(http, worker_id, "ACTIVE")
            cluster.stop_worker_gracefully(0)
            assert cluster.worker_alive(0) is False
            wait_log_contains(cluster.worker_log_path(0), "event=worker_stopped_registered")
            item = _wait_worker(http, worker_id, "STOPPED")
            assert item["is_alive"] is False
            assert item["stopped_at"] is not None
        client_redis = _redis()
        try:
            assert int(client_redis.exists(heartbeat_key(worker_id))) == 0
        finally:
            client_redis.close()
        with _pg() as conn:
            stopped_at = conn.execute(
                "SELECT stopped_at FROM workers WHERE id = %s",
                (worker_id,),
            ).fetchone()
        assert stopped_at is not None and stopped_at[0] is not None
        print(f"SIGTERM worker_id={worker_id} stopped_at={stopped_at[0]}")
    finally:
        cluster.stop_all()


def test_graceful_sigterm_finishes_running_job(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_publisher()
        cluster.start_workers(1)
        worker_id = cluster.worker_ids()[0]
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as http:
            created = http.post(
                "/jobs",
                json={"job_type": "sleep", "payload": {"seconds": 1.5}},
            )
            assert created.status_code == 202
            job_id = created.json()["id"]
            deadline = time.monotonic() + 8.0
            while time.monotonic() < deadline:
                body = http.get(f"/jobs/{job_id}").json()
                if body["status"] == "RUNNING":
                    break
                time.sleep(0.05)
            else:
                raise AssertionError("job never reached RUNNING")
            cluster.stop_worker_gracefully(0, timeout=20.0)
            done = http.get(f"/jobs/{job_id}").json()
            assert done["status"] == "SUCCEEDED"
            item = _wait_worker(http, worker_id, "STOPPED")
            assert item["is_alive"] is False
        print(f"SIGTERM_DRAIN worker_id={worker_id} job={job_id}")
    finally:
        cluster.stop_all()


def test_sigkill_expires_heartbeat_without_stopped_at(
    redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_workers(1)
        worker_id = cluster.worker_ids()[0]
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as http:
            _wait_worker(http, worker_id, "ACTIVE")
            cluster.kill_worker_sigkill(0)
            client_redis = _redis()
            try:
                deadline = time.monotonic() + 5.0
                gone = False
                while time.monotonic() < deadline:
                    if int(client_redis.exists(heartbeat_key(worker_id))) == 0:
                        gone = True
                        break
                    time.sleep(0.05)
                assert gone is True
            finally:
                client_redis.close()
            with _pg() as conn:
                stopped_row = conn.execute(
                    "SELECT stopped_at FROM workers WHERE id = %s",
                    (worker_id,),
                ).fetchone()
            assert stopped_row is not None and stopped_row[0] is None
            item = _wait_worker(http, worker_id, "EXPIRED")
            assert item["is_alive"] is False
        print(f"SIGKILL_EXPIRED worker_id={worker_id}")
    finally:
        cluster.stop_all()


def test_worker_restart_creates_new_registry_row(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_workers(1)
        first_id = cluster.worker_ids()[0]
        cluster.stop_worker_gracefully(0)
        cluster.start_workers(1)
        second_id = cluster.worker_ids()[1]
        assert second_id != first_id
        with _pg() as conn:
            ids = [row[0] for row in conn.execute("SELECT id FROM workers").fetchall()]
        assert first_id in ids
        assert second_id in ids
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as http:
            body = http.get("/workers").json()
            by_id = {item["id"]: item for item in body["items"]}
            assert by_id[first_id]["status"] == "STOPPED"
            assert by_id[second_id]["status"] == "ACTIVE"
        print(f"RESTART old={first_id} new={second_id}")
    finally:
        cluster.stop_all()


def test_redis_outage_lists_unknown_then_resumes(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    redis_stopped = False
    try:
        cluster.start_api()
        cluster.start_workers(1)
        worker_id = cluster.worker_ids()[0]
        pid = cluster.worker_pid(0)
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as http:
            _wait_worker(http, worker_id, "ACTIVE")
            stop_project_redis()
            redis_stopped = True
            time.sleep(0.4)
            assert cluster.worker_alive(0)
            assert cluster.worker_pid(0) == pid
            body = http.get("/workers").json()
            assert body["liveness_available"] is False
            item = next(row for row in body["items"] if row["id"] == worker_id)
            assert item["status"] == "UNKNOWN"
            start_project_redis()
            redis_stopped = False
            recovered = _wait_worker(http, worker_id, "ACTIVE", timeout=15.0)
            assert recovered["is_alive"] is True
            assert cluster.worker_pid(0) == pid
        print(f"REDIS_OUTAGE worker_id={worker_id} pid={pid}")
    finally:
        cluster.stop_all()
        if redis_stopped:
            start_project_redis()


def test_worker_starts_while_postgres_is_down(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    postgres_stopped = False
    try:
        stop_project_postgres()
        postgres_stopped = True
        worker = cluster.start_worker(0)
        time.sleep(1.5)
        assert worker.proc.poll() is None
        pid = worker.proc.pid
        logs = worker.log_path.read_text(encoding="utf-8")
        assert "event=worker_ready" not in logs
        wait_log_contains(worker.log_path, "event=worker_registration_failed", timeout=10.0)
        start_project_postgres()
        postgres_stopped = False
        wait_log_contains(worker.log_path, "event=worker_ready", timeout=20.0)
        assert worker.proc.pid == pid
        cluster.start_api()
        cluster.start_publisher()
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as http:
            created = http.post(
                "/jobs",
                json={"job_type": "word_count", "payload": {"text": "after postgres"}},
            )
            assert created.status_code == 202
            job_id = created.json()["id"]
            deadline = time.monotonic() + 15.0
            status = None
            while time.monotonic() < deadline:
                body = http.get(f"/jobs/{job_id}").json()
                if body["status"] == "SUCCEEDED":
                    status = body["status"]
                    break
                time.sleep(0.05)
            assert status == "SUCCEEDED"
        print(f"POSTGRES_STARTUP pid={pid} job={job_id}")
    finally:
        cluster.stop_all()
        if postgres_stopped:
            start_project_postgres()


def test_last_seen_at_advances(redis_available: None, tmp_path: Path) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        cluster.start_api()
        cluster.start_workers(1)
        worker_id = cluster.worker_ids()[0]
        with _pg() as conn:
            first_row = conn.execute(
                "SELECT last_seen_at FROM workers WHERE id = %s",
                (worker_id,),
            ).fetchone()
        assert first_row is not None
        first = first_row[0]
        deadline = time.monotonic() + 5.0
        advanced = first
        while time.monotonic() < deadline:
            time.sleep(0.3)
            with _pg() as conn:
                advanced_row = conn.execute(
                    "SELECT last_seen_at FROM workers WHERE id = %s",
                    (worker_id,),
                ).fetchone()
            assert advanced_row is not None
            advanced = advanced_row[0]
            if advanced != first:
                break
        assert advanced != first
    finally:
        cluster.stop_all()
