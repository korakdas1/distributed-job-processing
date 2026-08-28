"""Start and stop local API/worker OS processes for integration tests."""

from __future__ import annotations

import os
import re
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import psycopg
import redis as redis_sync

from job_platform.core.config import get_settings

ROOT = Path(__file__).resolve().parents[2]
_WORKER_ID_RE = re.compile(r"worker_id=(worker-[0-9a-f]{8})")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def process_env() -> dict[str, str]:
    env = os.environ.copy()
    settings = get_settings()
    env["PYTHONPATH"] = str(ROOT / "src")
    env["POSTGRES_HOST"] = settings.postgres_host
    env["POSTGRES_PORT"] = str(settings.postgres_port)
    env["POSTGRES_DB"] = settings.postgres_db
    env["POSTGRES_USER"] = settings.postgres_user
    env["POSTGRES_PASSWORD"] = settings.postgres_password.get_secret_value()
    env["REDIS_HOST"] = settings.redis_host
    env["REDIS_PORT"] = str(settings.redis_port)
    env["REDIS_DB"] = str(settings.redis_db)
    env["WORKER_READ_BLOCK_MS"] = "200"
    env["LOG_LEVEL"] = "INFO"
    env["RETRY_BASE_DELAY_SECONDS"] = "0.15"
    env["RETRY_MAX_DELAY_SECONDS"] = "0.5"
    env["RETRY_JITTER_RATIO"] = "0"
    env["RETRY_SCHEDULER_POLL_INTERVAL_MS"] = "50"
    env["WORKER_HEARTBEAT_INTERVAL_SECONDS"] = "0.2"
    env["WORKER_HEARTBEAT_TTL_SECONDS"] = "1.0"
    env["WORKER_DB_HEARTBEAT_INTERVAL_SECONDS"] = "0.4"
    return env


def wait_http(url: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = "no response"
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=1.0)
            if response.status_code == 200:
                return
            last_error = f"status {response.status_code}"
        except Exception as exc:
            last_error = type(exc).__name__
        time.sleep(0.05)
    raise AssertionError(f"HTTP {url} did not become ready: {last_error}")


def wait_log_contains(path: Path, needle: str, timeout: float = 15.0) -> str:
    deadline = time.monotonic() + timeout
    text = ""
    while time.monotonic() < deadline:
        if path.exists():
            text = path.read_text(encoding="utf-8")
            if needle in text:
                return text
        time.sleep(0.05)
    raise AssertionError(f"Did not find {needle!r} in {path} within {timeout}s:\n{text[-2000:]}")


def parse_worker_id(log_text: str) -> str:
    match = _WORKER_ID_RE.search(log_text)
    if match is None:
        raise AssertionError(f"No worker_id in logs:\n{log_text[-2000:]}")
    return match.group(1)


def kill_process_sigkill(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    proc.wait(timeout=5)


def stop_process(proc: subprocess.Popen[str], *, sig: int = signal.SIGTERM) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, sig)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=5)


def _redis_ping() -> bool:
    settings = get_settings()
    client = redis_sync.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
        socket_connect_timeout=1,
        socket_timeout=1,
    )
    try:
        return bool(client.ping())
    except Exception:
        return False
    finally:
        client.close()


def _compose(*args: str) -> None:
    """Operate on this repository's Compose project only. Never prune or down -v."""
    subprocess.run(
        ["docker-compose", "-p", ROOT.name, *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, "COMPOSE_PROJECT_NAME": ROOT.name},
    )


def stop_project_redis() -> None:
    """Stop only this project's Compose Redis service. Volumes are kept."""
    _compose("stop", "redis")
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        if not _redis_ping():
            return
        time.sleep(0.1)
    raise AssertionError("Compose redis service did not stop accepting connections")


def start_project_redis() -> None:
    """Start this project's Compose Redis service and wait until PING succeeds."""
    _compose("start", "redis")
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        if _redis_ping():
            return
        time.sleep(0.1)
    raise AssertionError("Compose redis service did not become reachable after start")


def _postgres_ready() -> bool:
    settings = get_settings()
    try:
        with psycopg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password.get_secret_value(),
            connect_timeout=1,
        ) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


def stop_project_postgres() -> None:
    """Stop only this project's Compose PostgreSQL service. Volumes are kept."""
    _compose("stop", "postgres")
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        if not _postgres_ready():
            return
        time.sleep(0.1)
    raise AssertionError("Compose postgres service did not stop accepting connections")


def start_project_postgres() -> None:
    """Start this project's Compose PostgreSQL service and wait until SELECT 1 succeeds."""
    _compose("start", "postgres")
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        if _postgres_ready():
            return
        time.sleep(0.1)
    raise AssertionError("Compose postgres service did not become reachable after start")


@dataclass
class ManagedProcess:
    proc: subprocess.Popen[str]
    log_path: Path
    handle: object


class ProcessCluster:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.env = process_env()
        self.port = free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self._api: ManagedProcess | None = None
        self._publisher: ManagedProcess | None = None
        self._scheduler: ManagedProcess | None = None
        self._workers: list[ManagedProcess] = []

    def start_api(self) -> ManagedProcess:
        log_path = self.tmp_path / "api.log"
        handle = log_path.open("w", encoding="utf-8", buffering=1)
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "job_platform.api.app:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--log-level",
                "info",
            ],
            cwd=ROOT,
            env=self.env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        self._api = ManagedProcess(proc=proc, log_path=log_path, handle=handle)
        wait_http(f"{self.base_url}/health")
        return self._api

    def stop_api(self) -> None:
        if self._api is None:
            return
        stop_process(self._api.proc)
        self._api.handle.close()
        self._api = None

    def start_publisher(self) -> ManagedProcess:
        log_path = self.tmp_path / "publisher.log"
        handle = log_path.open("w", encoding="utf-8", buffering=1)
        proc = subprocess.Popen(
            [sys.executable, "-m", "job_platform.outbox"],
            cwd=ROOT,
            env=self.env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        self._publisher = ManagedProcess(proc=proc, log_path=log_path, handle=handle)
        wait_log_contains(self._publisher.log_path, "event=publisher_ready")
        return self._publisher

    def publisher_pid(self) -> int:
        if self._publisher is None:
            raise AssertionError("Publisher process is not running")
        return self._publisher.proc.pid

    def publisher_alive(self) -> bool:
        return self._publisher is not None and self._publisher.proc.poll() is None

    def stop_publisher(self) -> None:
        if self._publisher is None:
            return
        stop_process(self._publisher.proc)
        self._publisher.handle.close()
        self._publisher = None

    def start_scheduler(self) -> ManagedProcess:
        log_path = self.tmp_path / "scheduler.log"
        handle = log_path.open("w", encoding="utf-8", buffering=1)
        proc = subprocess.Popen(
            [sys.executable, "-m", "job_platform.scheduler"],
            cwd=ROOT,
            env=self.env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        self._scheduler = ManagedProcess(proc=proc, log_path=log_path, handle=handle)
        wait_log_contains(self._scheduler.log_path, "event=scheduler_ready")
        return self._scheduler

    def scheduler_pid(self) -> int:
        if self._scheduler is None:
            raise AssertionError("Scheduler process is not running")
        return self._scheduler.proc.pid

    def scheduler_alive(self) -> bool:
        return self._scheduler is not None and self._scheduler.proc.poll() is None

    def stop_scheduler(self) -> None:
        if self._scheduler is None:
            return
        stop_process(self._scheduler.proc)
        self._scheduler.handle.close()
        self._scheduler = None

    def start_worker(self, index: int) -> ManagedProcess:
        log_path = self.tmp_path / f"worker-{index}.log"
        handle = log_path.open("w", encoding="utf-8", buffering=1)
        proc = subprocess.Popen(
            [sys.executable, "-m", "job_platform.worker"],
            cwd=ROOT,
            env=self.env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        managed = ManagedProcess(proc=proc, log_path=log_path, handle=handle)
        self._workers.append(managed)
        return managed

    def start_workers(self, count: int) -> list[ManagedProcess]:
        started = [self.start_worker(len(self._workers)) for _ in range(count)]
        for worker in started:
            wait_log_contains(worker.log_path, "event=worker_ready")
        return started

    def api_pid(self) -> int:
        if self._api is None:
            raise AssertionError("API process is not running")
        return self._api.proc.pid

    def worker_ids(self) -> list[str]:
        return [
            parse_worker_id(item.log_path.read_text(encoding="utf-8")) for item in self._workers
        ]

    def worker_pid(self, index: int = 0) -> int:
        return self._workers[index].proc.pid

    def worker_log_path(self, index: int = 0) -> Path:
        return self._workers[index].log_path

    def worker_alive(self, index: int = 0) -> bool:
        return self._workers[index].proc.poll() is None

    def stop_worker_gracefully(self, index: int = 0, *, timeout: float = 15.0) -> ManagedProcess:
        worker = self._workers[index]
        if worker.proc.poll() is None:
            try:
                os.killpg(worker.proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                return worker
            try:
                worker.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(worker.proc.pid, signal.SIGKILL)
                worker.proc.wait(timeout=5)
        return worker

    def worker_pids(self) -> list[int]:
        return [item.proc.pid for item in self._workers]

    def kill_worker_sigkill(self, index: int = 0) -> ManagedProcess:
        worker = self._workers[index]
        kill_process_sigkill(worker.proc)
        return worker

    def stop_all(self) -> None:
        for worker in self._workers:
            stop_process(worker.proc)
            worker.handle.close()
        self.stop_scheduler()
        self.stop_publisher()
        if self._api is not None:
            stop_process(self._api.proc)
            self._api.handle.close()
