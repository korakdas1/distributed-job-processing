"""Prove the worker is a separate OS process from the API."""

from __future__ import annotations

import time
from pathlib import Path

import httpx
import pytest

from tests.integration.process_harness import ProcessCluster

pytestmark = pytest.mark.integration


def test_worker_process_executes_job_not_api(
    redis_available: None,
    tmp_path: Path,
) -> None:
    cluster = ProcessCluster(tmp_path)
    try:
        api = cluster.start_api()
        cluster.start_publisher()
        workers = cluster.start_workers(1)
        worker = workers[0]
        with httpx.Client(base_url=cluster.base_url, timeout=10.0) as client:
            response = client.post(
                "/jobs",
                json={"job_type": "word_count", "payload": {"text": "process split"}},
            )
            assert response.status_code == 202, response.text
            job_id = response.json()["id"]
            body: dict[str, object] = {}
            deadline = time.monotonic() + 15.0
            while time.monotonic() < deadline:
                body = client.get(f"/jobs/{job_id}").json()
                if body.get("status") == "SUCCEEDED":
                    break
                time.sleep(0.1)
            assert body.get("status") == "SUCCEEDED"
            assert body.get("result") == {"word_count": 2}
            assert api.proc.poll() is None
            assert worker.proc.poll() is None
            assert api.proc.pid != worker.proc.pid
    finally:
        cluster.stop_all()

    worker_logs = worker.log_path.read_text(encoding="utf-8")
    api_logs = (tmp_path / "api.log").read_text(encoding="utf-8")
    assert "event=task_started" in worker_logs
    assert "event=task_succeeded" in worker_logs
    assert "event=task_started" not in api_logs
