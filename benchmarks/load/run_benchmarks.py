#!/usr/bin/env python3
"""Isolated Compose performance baseline.

Operates only on a project name starting with job-platform-load-bench.
Never prunes Docker. Never downs the development project. Never FLUSHALL.
Importing this module does not start Docker or submit jobs.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import platform
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import psycopg

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from benchmarks.load import stats as bench_stats
from benchmarks.load.workloads import (
    FULL,
    PRIME_LIMIT,
    QUICK,
    SLEEP_SECONDS,
    WORD_COUNT_TEXT,
    ProfileConfig,
    prime_body,
    sleep_body,
    word_count_body,
)

PROJECT = os.environ.get("LOAD_BENCH_COMPOSE_PROJECT", "job-platform-load-bench")
API_HOST_PORT = os.environ.get("LOAD_BENCH_API_HOST_PORT", "18002")
POSTGRES_HOST_PORT = os.environ.get("LOAD_BENCH_POSTGRES_HOST_PORT", "15435")
REDIS_HOST_PORT = os.environ.get("LOAD_BENCH_REDIS_HOST_PORT", "16381")
BASE_URL = f"http://127.0.0.1:{API_HOST_PORT}"
RESULTS_DIR = Path(__file__).resolve().parent / "results"
BENCHMARK_SCHEMA_VERSION = 1
POLL_SECONDS = 0.4
STATS_INTERVAL = 1.0
DEV_PROJECT = "distributed-job-processing-platform"


def _port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex(("127.0.0.1", port)) != 0


def _load_env_file() -> dict[str, str]:
    values: dict[str, str] = {}
    path = ROOT / ".env"
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _compose_env() -> dict[str, str]:
    env = os.environ.copy()
    env["API_HOST_PORT"] = API_HOST_PORT
    env["POSTGRES_HOST_PORT"] = POSTGRES_HOST_PORT
    env["REDIS_HOST_PORT"] = REDIS_HOST_PORT
    env["COMPOSE_PROJECT_NAME"] = PROJECT
    return env


def fail(message: str, proc: subprocess.CompletedProcess[str] | None = None) -> None:
    if proc is not None and proc.stderr:
        message = f"{message}\n{proc.stderr[-4000:]}"
    raise SystemExit(message)


def assert_project_safe() -> None:
    if not PROJECT.startswith("job-platform-load-bench"):
        fail(f"Refusing project {PROJECT!r}; must start with job-platform-load-bench")


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        ["docker-compose", "-p", PROJECT, *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=_compose_env(),
    )
    if check and proc.returncode != 0:
        fail(f"docker-compose {' '.join(args)} failed with {proc.returncode}", proc)
    return proc


def run_cmd(args: Sequence[str]) -> str:
    proc = subprocess.run(args, check=False, capture_output=True, text=True)
    return (proc.stdout or "").strip()


def development_stack_running() -> bool:
    proc = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.project={DEV_PROJECT}",
            "--format",
            "{{.Names}}",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    return bool(proc.stdout.strip())


def pg() -> psycopg.Connection:
    file_env = _load_env_file()
    password = (
        os.environ.get("POSTGRES_PASSWORD")
        or file_env.get("POSTGRES_PASSWORD")
        or "change-me-in-local-env"
    )
    return psycopg.connect(
        host="127.0.0.1",
        port=int(POSTGRES_HOST_PORT),
        dbname=file_env.get("POSTGRES_DB", os.environ.get("POSTGRES_DB", "job_platform")),
        user=file_env.get("POSTGRES_USER", os.environ.get("POSTGRES_USER", "job_platform")),
        password=password,
        connect_timeout=5,
    )


def wait_http(path: str, *, timeout: float = 90.0, status: int = 200) -> httpx.Response:
    deadline = time.monotonic() + timeout
    last = "no response"
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{BASE_URL}{path}", timeout=3.0)
            if response.status_code == status:
                return response
            last = f"status {response.status_code}"
        except Exception as exc:
            last = type(exc).__name__
        time.sleep(0.4)
    diagnostics()
    fail(f"{path} did not become HTTP {status}: {last}")
    raise AssertionError("unreachable")


def wait_ready(*, allow_degraded: bool, timeout: float = 90.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{BASE_URL}/ready", timeout=3.0)
            if response.status_code == 200:
                body = response.json()
                last = body
                if allow_degraded or body.get("degraded") is False:
                    return body
        except Exception:
            pass
        time.sleep(0.4)
    diagnostics()
    fail(f"/ready never ready (allow_degraded={allow_degraded}): {last}")
    raise AssertionError("unreachable")


def wait_active(count: int, *, timeout: float = 60.0) -> list[str]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            payload = httpx.get(f"{BASE_URL}/workers", timeout=5.0).json()
            items = payload.get("items", [])
            active = [item["id"] for item in items if item.get("status") == "ACTIVE"]
            if len(active) == count:
                return active
        except Exception:
            pass
        time.sleep(0.4)
    diagnostics()
    fail(f"expected {count} ACTIVE workers")
    raise AssertionError("unreachable")


def worker_container_count() -> int:
    proc = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.project={PROJECT}",
            "--filter",
            "label=com.docker.compose.service=worker",
            "--format",
            "{{.ID}}",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    return len([line for line in proc.stdout.splitlines() if line.strip()])


def scale_workers(count: int) -> list[str]:
    compose("up", "-d", "--scale", f"worker={count}")
    ids = wait_active(count)
    if worker_container_count() != count:
        fail(f"worker container count != {count}")
    return ids


def unpublished_count() -> int:
    with pg() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL"
        ).fetchone()
    return int(row[0]) if row else 0


def wait_unpublished_zero(*, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if unpublished_count() == 0:
            return
        time.sleep(0.4)
    fail("unpublished outbox did not drain")


def alembic_head() -> str:
    with pg() as conn:
        row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
    if row is None:
        fail("alembic_version missing")
    return str(row[0])


def diagnostics() -> None:
    print("BENCH_DIAG docker-compose ps")
    print(compose("ps", check=False).stdout)
    for path in ("/health", "/ready", "/metrics/summary", "/workers"):
        try:
            response = httpx.get(f"{BASE_URL}{path}", timeout=3.0)
            print(f"BENCH_DIAG {path} {response.status_code} {response.text[:400]}")
        except Exception as exc:
            print(f"BENCH_DIAG {path} {type(exc).__name__}")
    try:
        print(f"BENCH_DIAG unpublished={unpublished_count()}")
    except Exception as exc:
        print(f"BENCH_DIAG unpublished {type(exc).__name__}")
    for service in ("api", "publisher", "scheduler", "worker"):
        logs = compose("logs", "--tail=40", service, check=False)
        if logs.stdout:
            print(f"BENCH_DIAG logs {service}\n{logs.stdout[-2000:]}")


def collect_environment() -> dict[str, Any]:
    cpu_model = ""
    cores = os.cpu_count()
    lscpu = run_cmd(["bash", "-lc", "lscpu 2>/dev/null | awk -F: '/Model name/{print $2; exit}'"])
    cpu_model = lscpu.strip()
    mem = run_cmd(["bash", "-lc", "awk '/MemTotal/{print $2}' /proc/meminfo"])
    ram_mib = round(int(mem) / 1024.0, 1) if mem.isdigit() else None
    load = os.getloadavg() if hasattr(os, "getloadavg") else (None, None, None)
    images = run_cmd(
        [
            "bash",
            "-lc",
            "docker image inspect job-platform-app:local --format '{{.Id}}' 2>/dev/null | head -1",
        ]
    )
    pg_digest_cmd = (
        "docker image inspect postgres:16-alpine "
        "--format '{{.RepoDigests}}' 2>/dev/null | head -c 80"
    )
    pg_image = run_cmd(["bash", "-lc", pg_digest_cmd])
    return {
        "captured_at": datetime.now(UTC).isoformat(),
        "os": platform.system(),
        "kernel": platform.release(),
        "python": platform.python_version(),
        "cpu_model": cpu_model,
        "logical_cpus": cores,
        "ram_mib": ram_mib,
        "loadavg_1_5_15": list(load),
        "docker": run_cmd(["docker", "--version"]),
        "docker_compose": run_cmd(["docker-compose", "version"]),
        "app_image": "job-platform-app:local",
        "app_image_id": images,
        "postgres_image": "postgres:16-alpine",
        "postgres_image_note": pg_image,
        "redis_image": "redis:7-alpine",
        "timing": {
            "JOB_LEASE_TIMEOUT_SECONDS": "60 (Compose default; not shortened)",
            "WORKER_HEARTBEAT_INTERVAL_SECONDS": "2",
            "WORKER_HEARTBEAT_TTL_SECONDS": "6",
        },
        "sqlalchemy_pool": (
            "create_async_engine default (pool_pre_ping=True; no load-harness tuning)"
        ),
        "uvicorn": "single process exec uvicorn ... --host 0.0.0.0 --port 8000 (no --workers)",
        "local_machine_caveat": (
            "Results are from one local development machine using Docker Compose. "
            "They are not production capacity."
        ),
    }


class ResourceSampler:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.samples: list[dict[str, Any]] = []
        self._label_cache: dict[str, str] = {}

    def start(self) -> None:
        self.samples = []
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="load-stats", daemon=True)
        self._thread.start()

    def stop(self) -> list[dict[str, Any]]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        return list(self.samples)

    def _service_for(self, container_id: str) -> str | None:
        if container_id in self._label_cache:
            return self._label_cache[container_id]
        fmt_project = '{{index .Config.Labels "com.docker.compose.project"}}'
        fmt_service = '{{index .Config.Labels "com.docker.compose.service"}}'
        project = run_cmd(["docker", "inspect", "-f", fmt_project, container_id])
        service = run_cmd(["docker", "inspect", "-f", fmt_service, container_id])
        if project != PROJECT:
            self._label_cache[container_id] = ""
            return None
        self._label_cache[container_id] = service
        return service

    def _loop(self) -> None:
        while not self._stop.is_set():
            proc = subprocess.run(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.ID}}\t{{.CPUPerc}}\t{{.MemUsage}}",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            snapshot: dict[str, Any] = {"ts": time.time()}
            worker_cpu = 0.0
            worker_mem = 0.0
            worker_n = 0
            for line in proc.stdout.splitlines():
                parts = line.split("\t")
                if len(parts) < 3:
                    continue
                container_id, cpu_raw, mem_raw = parts[0], parts[1], parts[2]
                service = self._service_for(container_id)
                if not service:
                    continue
                try:
                    cpu = bench_stats.parse_cpu_percent(cpu_raw)
                    mem = bench_stats.parse_mem_mib(mem_raw)
                except ValueError:
                    continue
                if service == "worker":
                    worker_cpu += cpu
                    worker_mem += mem
                    worker_n += 1
                else:
                    snapshot[service] = {"cpu_percent": cpu, "mem_mib": mem}
            if worker_n:
                snapshot["worker"] = {
                    "cpu_percent": worker_cpu,
                    "mem_mib": worker_mem,
                    "containers": worker_n,
                }
            self.samples.append(snapshot)
            self._stop.wait(STATS_INTERVAL)


def _ms(delta_seconds: float) -> float:
    return delta_seconds * 1000.0


async def submit_jobs(
    bodies: Sequence[Mapping[str, Any]],
    *,
    concurrency: int,
) -> tuple[list[dict[str, Any]], float]:
    limits = httpx.Limits(
        max_connections=max(concurrency, 4),
        max_keepalive_connections=max(concurrency, 4),
    )
    semaphore = asyncio.Semaphore(concurrency)
    results: list[dict[str, Any] | None] = [None] * len(bodies)

    async def one(index: int, body: Mapping[str, Any]) -> None:
        async with semaphore:
            started = time.perf_counter()
            try:
                response = await client.post("/jobs", json=dict(body))
                elapsed_ms = _ms(time.perf_counter() - started)
                job_id = None
                if response.status_code == 202:
                    job_id = str(response.json().get("id"))
                results[index] = {
                    "status_code": response.status_code,
                    "latency_ms": elapsed_ms,
                    "job_id": job_id,
                }
            except Exception as exc:
                results[index] = {
                    "status_code": 0,
                    "latency_ms": _ms(time.perf_counter() - started),
                    "job_id": None,
                    "error": type(exc).__name__,
                }

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0, limits=limits) as client:
        wall_start = time.perf_counter()
        await asyncio.gather(*[one(i, body) for i, body in enumerate(bodies)])
        wall_s = time.perf_counter() - wall_start
    return [item for item in results if item is not None], wall_s


def wait_jobs_terminal(job_ids: Sequence[str], *, timeout: float) -> dict[str, int]:
    if not job_ids:
        return {}
    deadline = time.monotonic() + timeout
    ids = list(job_ids)
    last: dict[str, int] = {}
    while time.monotonic() < deadline:
        with pg() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) FROM jobs WHERE id = ANY(%s) GROUP BY status",
                (ids,),
            ).fetchall()
        last = {str(status): int(count) for status, count in rows}
        terminal = last.get("SUCCEEDED", 0) + last.get("FAILED", 0) + last.get("CANCELLED", 0)
        if terminal == len(ids) and sum(last.values()) == len(ids):
            return last
        time.sleep(POLL_SECONDS)
    diagnostics()
    fail(f"jobs did not all reach terminal state: {last} expected {len(ids)}")
    raise AssertionError("unreachable")


def fetch_job_metrics(job_ids: Sequence[str]) -> list[dict[str, Any]]:
    with pg() as conn:
        rows = conn.execute(
            """
            SELECT j.id, j.status, j.priority, j.created_at, j.queued_at, j.started_at,
                   j.completed_at, j.attempt_count, j.max_attempts, j.worker_id,
                   a.started_at, a.finished_at, a.duration_ms, a.status, a.worker_id
            FROM jobs j
            LEFT JOIN job_attempts a
              ON a.job_id = j.id AND a.attempt_number = 1
            WHERE j.id = ANY(%s)
            """,
            (list(job_ids),),
        ).fetchall()
    metrics: list[dict[str, Any]] = []
    for row in rows:
        created, queued, job_started, completed = row[3], row[4], row[5], row[6]
        attempt_started, attempt_finished, duration_ms = row[10], row[11], row[12]
        queue_ms = None
        if queued is not None and attempt_started is not None:
            queue_ms = (attempt_started - queued).total_seconds() * 1000.0
        elif queued is not None and job_started is not None:
            queue_ms = (job_started - queued).total_seconds() * 1000.0
        exec_ms = None
        if duration_ms is not None:
            exec_ms = float(duration_ms)
        elif attempt_started is not None and attempt_finished is not None:
            exec_ms = (attempt_finished - attempt_started).total_seconds() * 1000.0
        e2e_ms = None
        if created is not None and completed is not None:
            e2e_ms = (completed - created).total_seconds() * 1000.0
        metrics.append(
            {
                "id": str(row[0]),
                "status": row[1],
                "priority": row[2],
                "attempt_count": int(row[7]),
                "max_attempts": int(row[8]),
                "job_worker_id": row[9],
                "attempt_worker_id": row[14],
                "queue_ms": queue_ms,
                "exec_ms": exec_ms,
                "e2e_ms": e2e_ms,
            }
        )
    return metrics


def summarize_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = {}
    for row in rows:
        statuses[row["status"]] = statuses.get(row["status"], 0) + 1
    unexpected_retry = sum(1 for row in rows if int(row["attempt_count"]) != 1)
    workers = sorted(
        {str(row["attempt_worker_id"]) for row in rows if row.get("attempt_worker_id")}
    )
    return {
        "n": len(rows),
        "status_counts": statuses,
        "unexpected_retry_count": unexpected_retry,
        "attempt_count_gt_1": unexpected_retry,
        "queue": bench_stats.latency_summary(
            [float(row["queue_ms"]) for row in rows if row.get("queue_ms") is not None]
        ),
        "execution": bench_stats.latency_summary(
            [float(row["exec_ms"]) for row in rows if row.get("exec_ms") is not None]
        ),
        "e2e": bench_stats.latency_summary(
            [float(row["e2e_ms"]) for row in rows if row.get("e2e_ms") is not None]
        ),
        "worker_ids": workers,
        "valid_baseline": (
            statuses.get("SUCCEEDED", 0) == len(rows) and unexpected_retry == 0 and len(rows) > 0
        ),
    }


def resource_summary(samples: list[dict[str, Any]]) -> dict[str, Any]:
    services = ("api", "postgres", "redis", "publisher", "scheduler", "worker")
    return {name: bench_stats.resource_peaks(samples, name) for name in services}


def require_valid(summary: dict[str, Any], *, context: str) -> None:
    if not summary.get("valid_baseline"):
        fail(
            f"{context} is INVALID FOR PERFORMANCE BASELINE: "
            f"status={summary.get('status_counts')} retries={summary.get('unexpected_retry_count')}"
        )


def run_warmup(n: int) -> None:
    bodies = [word_count_body() for _ in range(n)]
    posts, _wall = asyncio.run(submit_jobs(bodies, concurrency=min(10, n)))
    ids = [str(item["job_id"]) for item in posts if item.get("job_id")]
    if len(ids) != n:
        fail(f"warmup POST mismatch {len(ids)} != {n}")
    wait_jobs_terminal(ids, timeout=60.0)
    wait_unpublished_zero(timeout=30.0)


def submission_family(cfg: ProfileConfig) -> list[dict[str, Any]]:
    print("BENCH_FAMILY submission (API + Postgres only; no publisher/workers)")
    compose("down", "-v", "--remove-orphans", check=False)
    compose("up", "-d", "postgres", "api")
    wait_http("/health")
    wait_ready(allow_degraded=True)
    head = alembic_head()
    if head != "0006_control_plane":
        fail(f"unexpected alembic {head}")
    print(f"BENCH_ALEMBIC {head} (head)")
    rows: list[dict[str, Any]] = []
    for conc in cfg.submission_conc:
        for repeat in range(1, cfg.repeats + 1):
            run_id = f"load-submission-c{conc}-r{repeat}"
            bodies = [word_count_body() for _ in range(cfg.submission_jobs)]
            sampler = ResourceSampler()
            sampler.start()
            posts, wall_s = asyncio.run(submit_jobs(bodies, concurrency=conc))
            samples = sampler.stop()
            ok = [item for item in posts if item["status_code"] == 202 and item.get("job_id")]
            errors = len(posts) - len(ok)
            ids = [str(item["job_id"]) for item in ok]
            with pg() as conn:
                job_n = conn.execute(
                    "SELECT COUNT(*) FROM jobs WHERE id = ANY(%s)", (ids,)
                ).fetchone()
                outbox_n = conn.execute(
                    """
                    SELECT COUNT(*) FROM outbox_events
                    WHERE job_id = ANY(%s) AND event_type = 'JOB_DISPATCH'
                    """,
                    (ids,),
                ).fetchone()
            job_count = int(job_n[0] if job_n else 0)
            outbox_count = int(outbox_n[0] if outbox_n else 0)
            if job_count != len(ok) or outbox_count != len(ok):
                fail(f"{run_id} durable job/outbox mismatch")
            post_lat = [float(item["latency_ms"]) for item in posts]
            rate = bench_stats.round_rate(len(ok) / wall_s) if wall_s > 0 else 0.0
            lat = bench_stats.latency_summary(post_lat)
            record = {
                "run_id": run_id,
                "family": "submission",
                "concurrency": conc,
                "repeat": repeat,
                "jobs_attempted": len(posts),
                "http_202": len(ok),
                "http_errors": errors,
                "wall_seconds": round(wall_s, 3),
                "submitted_jobs_per_sec": rate,
                "post_latency": lat,
                "resources": resource_summary(samples),
                "unpublished_after": unpublished_count(),
            }
            print(
                f"BENCH_SUBMISSION {run_id} 202={len(ok)} errors={errors} "
                f"rps={rate} p50={lat['p50']} p95={lat['p95']}"
            )
            rows.append(record)
    return rows


def e2e_batch(
    *,
    run_id: str,
    family: str,
    workers: int,
    bodies: Sequence[Mapping[str, Any]],
    concurrency: int,
    timeout: float,
) -> dict[str, Any]:
    active = scale_workers(workers)
    wait_ready(allow_degraded=False)
    wait_unpublished_zero(timeout=30.0)
    sampler = ResourceSampler()
    sampler.start()
    posts, submit_wall = asyncio.run(submit_jobs(bodies, concurrency=concurrency))
    ok = [item for item in posts if item["status_code"] == 202 and item.get("job_id")]
    if len(ok) != len(bodies):
        sampler.stop()
        fail(f"{run_id} HTTP errors {len(bodies) - len(ok)}")
    ids = [str(item["job_id"]) for item in ok]
    complete_start = time.perf_counter()
    wait_jobs_terminal(ids, timeout=timeout)
    complete_wall = time.perf_counter() - complete_start
    wait_unpublished_zero(timeout=30.0)
    samples = sampler.stop()
    metrics = fetch_job_metrics(ids)
    if len(metrics) != len(ids):
        fail(f"{run_id} missing job rows {len(metrics)} != {len(ids)}")
    summary = summarize_metrics(metrics)
    require_valid(summary, context=run_id)
    e2e_wall = submit_wall + complete_wall
    completed_rps = bench_stats.round_rate(len(ids) / e2e_wall) if e2e_wall > 0 else 0.0
    submitted_rps = bench_stats.round_rate(len(ok) / submit_wall) if submit_wall > 0 else 0.0
    record = {
        "run_id": run_id,
        "family": family,
        "workers": workers,
        "active_worker_ids": active,
        "jobs": len(ids),
        "http_202": len(ok),
        "http_errors": 0,
        "submit_wall_seconds": round(submit_wall, 3),
        "e2e_wall_seconds": round(e2e_wall, 3),
        "submitted_jobs_per_sec": submitted_rps,
        "completed_jobs_per_sec": completed_rps,
        "post_latency": bench_stats.latency_summary([float(item["latency_ms"]) for item in posts]),
        "queue": summary["queue"],
        "execution": summary["execution"],
        "e2e": summary["e2e"],
        "valid_baseline": summary["valid_baseline"],
        "unexpected_retry_count": summary["unexpected_retry_count"],
        "status_counts": summary["status_counts"],
        "worker_ids": summary["worker_ids"],
        "resources": resource_summary(samples),
        "unpublished_after": unpublished_count(),
    }
    print(
        f"BENCH_E2E {run_id} complete_rps={completed_rps} "
        f"queue_p50={summary['queue']['p50']} e2e_p50={summary['e2e']['p50']} "
        f"workers_used={len(summary['worker_ids'])}"
    )
    return record


def scaling_table(runs: list[dict[str, Any]], family: str) -> list[dict[str, Any]]:
    by_workers: dict[int, list[float]] = {}
    for run in runs:
        if run.get("family") != family:
            continue
        by_workers.setdefault(int(run["workers"]), []).append(float(run["completed_jobs_per_sec"]))
    if 1 not in by_workers:
        return []
    t1 = sorted(by_workers[1])[len(by_workers[1]) // 2]
    rows = []
    for workers, rates in sorted(by_workers.items()):
        ordered = sorted(rates)
        median = ordered[len(ordered) // 2]
        sp = bench_stats.speedup(median, t1)
        rows.append(
            {
                "workers": workers,
                "repeats": len(rates),
                "median_completed_jobs_per_sec": bench_stats.round_rate(median),
                "min_completed_jobs_per_sec": bench_stats.round_rate(min(rates)),
                "max_completed_jobs_per_sec": bench_stats.round_rate(max(rates)),
                "speedup_vs_1": None if sp is None else round(sp, 2),
                "efficiency_vs_1": None
                if bench_stats.efficiency(sp, workers) is None
                else round(float(bench_stats.efficiency(sp, workers)), 2),
            }
        )
    return rows


def priority_family(cfg: ProfileConfig) -> dict[str, Any]:
    run_id = "load-priority-w4-r1"
    workers = 4 if 4 in cfg.workers else max(cfg.workers)
    scale_workers(workers)
    wait_ready(allow_degraded=False)
    bodies: list[dict[str, object]] = []
    for priority, count in cfg.priority_mix.items():
        bodies.extend(sleep_body(priority=priority) for _ in range(count))
    sampler = ResourceSampler()
    sampler.start()
    posts, submit_wall = asyncio.run(submit_jobs(bodies, concurrency=min(25, len(bodies))))
    ok = [item for item in posts if item.get("job_id")]
    ids = [str(item["job_id"]) for item in ok]
    wait_jobs_terminal(ids, timeout=cfg.sleep_timeout_seconds)
    wait_unpublished_zero()
    samples = sampler.stop()
    metrics = fetch_job_metrics(ids)
    require_valid(summarize_metrics(metrics), context=run_id)
    by_pri: dict[str, list[dict[str, Any]]] = {}
    for row in metrics:
        by_pri.setdefault(str(row["priority"]), []).append(row)
    table = []
    for priority in ("CRITICAL", "HIGH", "NORMAL", "LOW"):
        group = by_pri.get(priority, [])
        table.append(
            {
                "priority": priority,
                "n": len(group),
                "queue": bench_stats.latency_summary(
                    [float(item["queue_ms"]) for item in group if item.get("queue_ms") is not None]
                ),
                "e2e": bench_stats.latency_summary(
                    [float(item["e2e_ms"]) for item in group if item.get("e2e_ms") is not None]
                ),
            }
        )
    low_progress = False
    # Reconstruct from timestamps: LOW started while any CRITICAL still not started.
    crit_rows = by_pri.get("CRITICAL", [])
    low_rows = by_pri.get("LOW", [])
    crit_starts = [row["queue_ms"] for row in crit_rows if row.get("queue_ms") is not None]
    low_starts = [row["queue_ms"] for row in low_rows if row.get("queue_ms") is not None]
    if crit_starts and low_starts:
        low_progress = min(low_starts) < max(crit_starts)
    print(f"BENCH_PRIORITY {run_id} low_progress_while_critical_queued={low_progress}")
    return {
        "run_id": run_id,
        "workers": workers,
        "jobs": len(ids),
        "submit_wall_seconds": round(submit_wall, 3),
        "by_priority": table,
        "low_progressed_while_higher_backlog": low_progress,
        "resources": resource_summary(samples),
        "valid_baseline": True,
    }


def calibrate_cpu() -> dict[str, Any]:
    scale_workers(1)
    wait_ready(allow_degraded=False)
    bodies = [prime_body(PRIME_LIMIT) for _ in range(3)]
    posts, _wall = asyncio.run(submit_jobs(bodies, concurrency=1))
    ids = [str(item["job_id"]) for item in posts if item.get("job_id")]
    wait_jobs_terminal(ids, timeout=60.0)
    metrics = fetch_job_metrics(ids)
    exec_ms = [float(row["exec_ms"]) for row in metrics if row.get("exec_ms") is not None]
    median = bench_stats.percentile(exec_ms, 50) if exec_ms else None
    note = "handler cap is 100000; target 100-500ms may be unreachable on this CPU"
    print(f"BENCH_CPU_CALIBRATION limit={PRIME_LIMIT} median_exec_ms={median}")
    return {
        "limit": PRIME_LIMIT,
        "samples": len(exec_ms),
        "median_exec_ms": None if median is None else round(median, 1),
        "note": note,
    }


def start_full_stack(workers: int) -> None:
    compose("down", "-v", "--remove-orphans", check=False)
    compose("up", "-d", "--scale", f"worker={workers}")
    wait_http("/health")
    wait_ready(allow_degraded=False)
    wait_active(workers)
    head = alembic_head()
    if head != "0006_control_plane":
        fail(f"unexpected alembic {head}")
    print(f"BENCH_ALEMBIC {head} (head)")


def write_artifacts(baseline: dict[str, Any], run_rows: list[dict[str, Any]]) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    json_path = RESULTS_DIR / "baseline.json"
    json_path.write_text(json.dumps(baseline, indent=2, default=str) + "\n", encoding="utf-8")
    csv_path = RESULTS_DIR / "runs.csv"
    fieldnames = [
        "run_id",
        "family",
        "workers",
        "concurrency",
        "jobs",
        "http_202",
        "http_errors",
        "submitted_jobs_per_sec",
        "completed_jobs_per_sec",
        "post_p50",
        "post_p95",
        "queue_p50",
        "queue_p95",
        "exec_p50",
        "exec_p95",
        "e2e_p50",
        "e2e_p95",
        "valid_baseline",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in run_rows:
            writer.writerow(
                {
                    "run_id": row.get("run_id"),
                    "family": row.get("family"),
                    "workers": row.get("workers", ""),
                    "concurrency": row.get("concurrency", ""),
                    "jobs": row.get("jobs") or row.get("jobs_attempted") or row.get("http_202"),
                    "http_202": row.get("http_202"),
                    "http_errors": row.get("http_errors"),
                    "submitted_jobs_per_sec": row.get("submitted_jobs_per_sec"),
                    "completed_jobs_per_sec": row.get("completed_jobs_per_sec", ""),
                    "post_p50": (row.get("post_latency") or {}).get("p50"),
                    "post_p95": (row.get("post_latency") or {}).get("p95"),
                    "queue_p50": (row.get("queue") or {}).get("p50"),
                    "queue_p95": (row.get("queue") or {}).get("p95"),
                    "exec_p50": (row.get("execution") or {}).get("p50"),
                    "exec_p95": (row.get("execution") or {}).get("p95"),
                    "e2e_p50": (row.get("e2e") or {}).get("p50"),
                    "e2e_p95": (row.get("e2e") or {}).get("p95"),
                    "valid_baseline": row.get("valid_baseline", ""),
                }
            )
    res_path = RESULTS_DIR / "resource_summary.csv"
    with res_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "run_id",
                "family",
                "service",
                "mean_cpu_percent",
                "peak_cpu_percent",
                "mean_mem_mib",
                "peak_mem_mib",
                "samples",
            ],
        )
        writer.writeheader()
        for row in run_rows:
            resources = row.get("resources") or {}
            for service, summary in resources.items():
                if not isinstance(summary, dict):
                    continue
                writer.writerow(
                    {
                        "run_id": row.get("run_id"),
                        "family": row.get("family"),
                        "service": service,
                        "mean_cpu_percent": summary.get("mean_cpu_percent"),
                        "peak_cpu_percent": summary.get("peak_cpu_percent"),
                        "mean_mem_mib": summary.get("mean_mem_mib"),
                        "peak_mem_mib": summary.get("peak_mem_mib"),
                        "samples": summary.get("samples"),
                    }
                )
    print(f"BENCH_WROTE {json_path}")
    print(f"BENCH_WROTE {csv_path}")
    print(f"BENCH_WROTE {res_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Isolated Compose performance baseline")
    parser.add_argument(
        "--profile",
        choices=("all", "submission", "lightweight", "sleep", "cpu", "priority"),
        default="all",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Small smoke sizes; not the canonical baseline",
    )
    parser.add_argument("--repeats", type=int, default=None, help="Override repeats (1-5)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    assert_project_safe()
    for port, label in (
        (int(API_HOST_PORT), "API"),
        (int(POSTGRES_HOST_PORT), "Postgres"),
        (int(REDIS_HOST_PORT), "Redis"),
    ):
        if not _port_free(port):
            fail(f"{label} host port {port} is already in use")
    cfg = QUICK if args.quick else FULL
    if args.repeats is not None:
        if args.repeats < 1 or args.repeats > 5:
            fail("--repeats must be 1-5")
        cfg = ProfileConfig(**{**cfg.__dict__, "repeats": args.repeats})
    wanted = (
        {args.profile}
        if args.profile != "all"
        else {
            "submission",
            "lightweight",
            "sleep",
            "cpu",
            "priority",
        }
    )
    env = collect_environment()
    env["development_stack_running"] = development_stack_running()
    env["project"] = PROJECT
    env["ports"] = {
        "api": API_HOST_PORT,
        "postgres": POSTGRES_HOST_PORT,
        "redis": REDIS_HOST_PORT,
    }
    env["profile"] = cfg.name
    env["word_count_chars"] = len(WORD_COUNT_TEXT)
    env["sleep_seconds"] = SLEEP_SECONDS
    print(f"BENCH_PROJECT {PROJECT}")
    print(f"BENCH_PORTS api={API_HOST_PORT} postgres={POSTGRES_HOST_PORT} redis={REDIS_HOST_PORT}")
    print(f"BENCH_PROFILE {cfg.name} repeats={cfg.repeats} quick={args.quick}")
    print(f"BENCH_DEV_STACK_RUNNING {env['development_stack_running']}")
    compose("build")
    all_runs: list[dict[str, Any]] = []
    submission_runs: list[dict[str, Any]] = []
    lightweight_runs: list[dict[str, Any]] = []
    sleep_runs: list[dict[str, Any]] = []
    cpu_runs: list[dict[str, Any]] = []
    priority_result: dict[str, Any] | None = None
    cpu_cal: dict[str, Any] | None = None
    final_health: dict[str, Any] = {}
    try:
        if "submission" in wanted:
            submission_runs = submission_family(cfg)
            all_runs.extend(submission_runs)
        e2e_wanted = wanted & {"lightweight", "sleep", "cpu", "priority"}
        if e2e_wanted:
            start_full_stack(max(cfg.workers))
            run_warmup(cfg.warmup_jobs)
            if "lightweight" in wanted:
                for workers in cfg.workers:
                    for repeat in range(1, cfg.repeats + 1):
                        run_id = f"load-lightweight-w{workers}-r{repeat}"
                        bodies = [word_count_body() for _ in range(cfg.lightweight_jobs)]
                        lightweight_runs.append(
                            e2e_batch(
                                run_id=run_id,
                                family="lightweight",
                                workers=workers,
                                bodies=bodies,
                                concurrency=cfg.lightweight_conc,
                                timeout=cfg.run_timeout_seconds,
                            )
                        )
                all_runs.extend(lightweight_runs)
            if "sleep" in wanted:
                for workers in cfg.workers:
                    for repeat in range(1, cfg.repeats + 1):
                        run_id = f"load-sleep-w{workers}-r{repeat}"
                        bodies = [sleep_body() for _ in range(cfg.sleep_jobs)]
                        sleep_runs.append(
                            e2e_batch(
                                run_id=run_id,
                                family="sleep",
                                workers=workers,
                                bodies=bodies,
                                concurrency=min(20, cfg.sleep_jobs),
                                timeout=cfg.sleep_timeout_seconds,
                            )
                        )
                all_runs.extend(sleep_runs)
            if "cpu" in wanted:
                cpu_cal = calibrate_cpu()
                for workers in cfg.workers:
                    for repeat in range(1, cfg.repeats + 1):
                        run_id = f"load-cpu-w{workers}-r{repeat}"
                        bodies = [prime_body() for _ in range(cfg.cpu_jobs)]
                        cpu_runs.append(
                            e2e_batch(
                                run_id=run_id,
                                family="cpu",
                                workers=workers,
                                bodies=bodies,
                                concurrency=min(20, cfg.cpu_jobs),
                                timeout=cfg.run_timeout_seconds,
                            )
                        )
                all_runs.extend(cpu_runs)
            if "priority" in wanted:
                priority_result = priority_family(cfg)
                all_runs.append(
                    {
                        "run_id": priority_result["run_id"],
                        "family": "priority",
                        "workers": priority_result["workers"],
                        "jobs": priority_result["jobs"],
                        "http_202": priority_result["jobs"],
                        "http_errors": 0,
                        "resources": priority_result["resources"],
                        "valid_baseline": True,
                    }
                )
            scale_workers(max(cfg.workers))
            wait_ready(allow_degraded=False)
            wait_unpublished_zero()
            health = httpx.get(f"{BASE_URL}/health", timeout=5.0)
            ready = httpx.get(f"{BASE_URL}/ready", timeout=5.0)
            summary = httpx.get(f"{BASE_URL}/metrics/summary", timeout=5.0)
            summary_status = (
                summary.json().get("status") if summary.status_code == 200 else summary.status_code
            )
            final_health = {
                "health": health.status_code,
                "ready": ready.status_code,
                "degraded": ready.json().get("degraded"),
                "summary": summary_status,
            }
            print(
                f"BENCH_FINAL health={health.status_code} ready={ready.status_code} "
                f"degraded={ready.json().get('degraded')} summary={final_health['summary']}"
            )
            ready_ok = health.status_code == 200 and ready.status_code == 200
            if not ready_ok or ready.json().get("degraded"):
                fail("final health/ready not clean")
        baseline = {
            "benchmark_schema_version": BENCHMARK_SCHEMA_VERSION,
            "suite": "load",
            "profile": cfg.name,
            "canonical_baseline": cfg.name == "full" and args.profile == "all",
            "environment": env,
            "definitions": {
                "submission_latency_ms": (
                    "client perf_counter immediately before POST until full HTTP response"
                ),
                "submitted_jobs_per_sec": ("HTTP 202 count / client submission wall-clock seconds"),
                "completed_jobs_per_sec": (
                    "SUCCEEDED measured jobs / (submit wall + wait-until-terminal wall)"
                ),
                "queue_latency_ms": "job_attempts.started_at - jobs.queued_at for attempt #1",
                "execution_duration_ms": (
                    "job_attempts.duration_ms (else finished_at - started_at)"
                ),
                "end_to_end_ms": "jobs.completed_at - jobs.created_at",
                "percentile": "linear interpolation at rank (p/100)*(n-1); p99 omitted if n<50",
                "error_rate_http": "non-202 POST count / attempted POSTs",
                "error_rate_jobs": "non-SUCCEEDED measured jobs / measured jobs",
                "http_reuse": (
                    "one httpx.AsyncClient per batch with keepalive; semaphore concurrency"
                ),
                "completion_observation": (
                    "batched read-only SQL every 400ms; no per-job GET polling"
                ),
            },
            "cpu_calibration": cpu_cal,
            "submission": submission_runs,
            "lightweight": lightweight_runs,
            "sleep": sleep_runs,
            "cpu": cpu_runs,
            "priority": priority_result,
            "scaling": {
                "lightweight": scaling_table(lightweight_runs, "lightweight"),
                "sleep": scaling_table(sleep_runs, "sleep"),
                "cpu": scaling_table(cpu_runs, "cpu"),
            },
            "final_health": final_health,
            "eight_workers": "skipped; 1/2/4 are the canonical scaling points",
            "payload": {
                "word_count_text_chars": len(WORD_COUNT_TEXT),
                "sleep_seconds": SLEEP_SECONDS,
                "prime_limit": PRIME_LIMIT,
            },
        }
        write_artifacts(baseline, all_runs)
        print("BENCH_SUITE_PASSED")
    except Exception:
        diagnostics()
        raise
    finally:
        compose("down", "-v", "--remove-orphans", check=False)
        print("BENCH_TEST_STACK_REMOVED")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        compose("down", "-v", "--remove-orphans", check=False)
        sys.exit(130)
