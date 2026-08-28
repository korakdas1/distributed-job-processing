"""Static Compose v1 checks. Does not start application containers."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.integration


def _compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker-compose", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def test_compose_config_lists_expected_services() -> None:
    result = _compose("config", "--services")
    assert set(result.stdout.split()) == {
        "postgres",
        "redis",
        "api",
        "publisher",
        "scheduler",
        "dashboard",
        "worker",
    }


def test_compose_worker_is_scalable_and_stateless() -> None:
    rendered = _compose("config").stdout
    body = rendered.split("worker:", 1)[1].split("\nvolumes:", 1)[0]
    assert "container_name:" not in body
    assert "ports:" not in body
    assert "privileged:" not in rendered
    assert "docker.sock" not in rendered
    assert "network_mode: host" not in rendered
    assert "POSTGRES_HOST: postgres" in rendered
    assert "REDIS_HOST: redis" in rendered
    print("COMPOSE_STATIC worker_scalable")


def test_compose_dashboard_is_loopback_and_stateless() -> None:
    rendered = _compose("config").stdout
    dash = rendered.split("\n  dashboard:\n", 1)[1].split("\n  postgres:\n", 1)[0]
    assert "127.0.0.1:" in dash
    assert "docker.sock" not in dash
    assert "privileged:" not in dash
    assert "volumes:" not in dash
    assert "job-platform-dashboard:local" in rendered
    assert "/var/run/docker.sock" not in rendered
