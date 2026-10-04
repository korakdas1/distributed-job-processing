"""Worker startup hardening when Redis is already down."""

from __future__ import annotations

import time
from pathlib import Path

import httpx
import pytest
import redis as redis_sync

from job_platform.core.config import get_settings
from job_platform.queue.priority import ready_streams
from tests.disposable_infrastructure import DisposableInfrastructure
from tests.integration.process_harness import (
    ProcessCluster,
    start_project_redis,
    stop_project_redis,
    wait_log_contains,
)

pytestmark = pytest.mark.integration


@pytest.mark.disruptive
def test_worker_starts_while_redis_is_down_and_resumes_without_restart(
    disposable_infrastructure: DisposableInfrastructure, redis_available: None, tmp_path: Path
) -> None:
    cluster = ProcessCluster(tmp_path)
    redis_stopped = False
    try:
        cluster.start_api()
        stop_project_redis(disposable_infrastructure)
        redis_stopped = True
        worker = cluster.start_worker(0)
        time.sleep(1.5)
        assert worker.proc.poll() is None
        pid = worker.proc.pid
        logs = worker.log_path.read_text(encoding="utf-8")
        assert "event=worker_ready" not in logs
        wait_log_contains(worker.log_path, "event=worker_queue_init_failed", timeout=10.0)
        assert worker.proc.poll() is None
        assert worker.proc.pid == pid

        start_project_redis(disposable_infrastructure)
        redis_stopped = False
        wait_log_contains(worker.log_path, "event=worker_ready", timeout=15.0)
        assert worker.proc.pid == pid
        settings = get_settings()
        client = redis_sync.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True,
            socket_connect_timeout=2,
        )
        try:
            for stream in ready_streams():
                groups = client.xinfo_groups(stream)
                names = [
                    str(item.get("name") or "") if isinstance(item, dict) else str(item[0])
                    for item in groups
                ]
                assert settings.redis_consumer_group in names, stream
        finally:
            client.close()

        cluster.start_publisher()
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as http:
            created = http.post(
                "/jobs",
                json={"job_type": "word_count", "payload": {"text": "after restore"}},
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
        print(f"WORKER_STARTUP_OUTAGE pid={pid} job={job_id}")
    finally:
        cluster.stop_all()
        if redis_stopped:
            start_project_redis(disposable_infrastructure)
