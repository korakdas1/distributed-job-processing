"""Static checks for the secure Compose overlay. Does not start containers."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PLACEHOLDER_PASSWORD = "secure-compose-config-only"
_SECURE_FILES = ["-f", "docker-compose.yml", "-f", "docker-compose.prod.yml"]

pytestmark = pytest.mark.integration


def _env_without_postgres_password() -> dict[str, str]:
    env = os.environ.copy()
    env.pop("POSTGRES_PASSWORD", None)
    return env


def _prod_config(*, password: str | None) -> subprocess.CompletedProcess[str]:
    env = _env_without_postgres_password()
    env_file = ROOT / "tmp" / "secure-compose-env"
    env_file.parent.mkdir(parents=True, exist_ok=True)
    if password is None:
        env_file.write_text("# no POSTGRES_PASSWORD\n", encoding="utf-8")
    else:
        env["POSTGRES_PASSWORD"] = password
        env_file.write_text(f"POSTGRES_PASSWORD={password}\n", encoding="utf-8")
    return subprocess.run(
        [
            "docker-compose",
            "--env-file",
            str(env_file),
            *_SECURE_FILES,
            "config",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


def _service_block(rendered: str, name: str) -> str:
    match = re.search(
        rf"\n  {re.escape(name)}:\n(.*?)(?=\n  [A-Za-z0-9_-]+:\n|\nvolumes:\n|\Z)",
        rendered,
        re.S,
    )
    if match is None:
        raise AssertionError(f"service {name!r} missing from compose config")
    return match.group(1)


def test_development_compose_keeps_password_fallback() -> None:
    result = subprocess.run(
        ["docker-compose", "-f", "docker-compose.yml", "config"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        env=_env_without_postgres_password(),
    )
    assert "change-me-in-local-env" in result.stdout
    print("COMPOSE_DEV_PASSWORD_FALLBACK ok")


def test_secure_compose_requires_postgres_password() -> None:
    result = _prod_config(password=None)
    assert result.returncode != 0
    combined = result.stderr + result.stdout
    assert "POSTGRES_PASSWORD" in combined
    print("COMPOSE_PROD_PASSWORD_REQUIRED ok")


def test_secure_compose_config_renders() -> None:
    result = _prod_config(password=COMPOSE_PLACEHOLDER_PASSWORD)
    assert result.returncode == 0, result.stderr[-2000:]
    rendered = result.stdout
    api = _service_block(rendered, "api")
    assert "VIEWER_API_KEY_FILE" in api
    assert "OPERATOR_API_KEY_FILE" in api
    assert "viewer_api_key" in api
    assert "operator_api_key" in api
    for name in ("publisher", "scheduler", "worker"):
        block = _service_block(rendered, name)
        assert "VIEWER_API_KEY" not in block
        assert "OPERATOR_API_KEY" not in block
        assert "viewer_api_key" not in block
        assert "operator_api_key" not in block
    assert "SECURITY_MODE: production" in rendered
    assert "cap_drop:" in rendered
    assert "read_only: true" in rendered
    assert "no-new-privileges" not in rendered
    assert "8443" in rendered
    assert "privileged: true" not in rendered
    assert "docker.sock" not in rendered
    assert "0.0.0.0:" not in rendered
    print("COMPOSE_PROD_STATIC ok")
