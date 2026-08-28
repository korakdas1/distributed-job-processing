from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from job_platform.api.app import create_app
from job_platform.core.config import (
    MIN_API_KEY_LENGTH,
    Settings,
    get_settings,
    validate_api_http_security,
)
from job_platform.security.auth import authenticate
from job_platform.security.secrets import read_secret_file, strip_one_trailing_newline

VIEWER = "a" * MIN_API_KEY_LENGTH
OPERATOR = "b" * MIN_API_KEY_LENGTH


@pytest.fixture(autouse=True)
def _clear_secret_file_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VIEWER_API_KEY_FILE", raising=False)
    monkeypatch.delenv("OPERATOR_API_KEY_FILE", raising=False)


def test_strip_one_trailing_newline() -> None:
    assert strip_one_trailing_newline("secret\n") == "secret"
    assert strip_one_trailing_newline("secret\r\n") == "secret"
    assert strip_one_trailing_newline("secret\n\n") == "secret\n"
    assert strip_one_trailing_newline("secret") == "secret"


def test_read_secret_file_strips_newline_and_rejects_empty(tmp_path: Path) -> None:
    path = tmp_path / "viewer_api_key"
    path.write_bytes(b"generated-secret-value-not-a-password\n")
    assert read_secret_file(str(path)) == "generated-secret-value-not-a-password"
    empty = tmp_path / "empty"
    empty.write_bytes(b"\n")
    with pytest.raises(ValueError, match="empty"):
        read_secret_file(str(empty))
    missing = tmp_path / "missing"
    with pytest.raises(ValueError, match="not found"):
        read_secret_file(str(missing))


def test_production_requires_strong_distinct_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SECURITY_MODE", "production")
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("VIEWER_API_KEY", VIEWER)
    monkeypatch.setenv("OPERATOR_API_KEY", OPERATOR)
    monkeypatch.delenv("VIEWER_API_KEY_FILE", raising=False)
    monkeypatch.delenv("OPERATOR_API_KEY_FILE", raising=False)
    settings = Settings()
    assert settings.effective_auth_enabled is True
    assert settings.effective_rate_limit_enabled is True
    assert settings.effective_docs_enabled is False


def test_production_rejects_auth_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SECURITY_MODE", "production")
    monkeypatch.setenv("AUTH_ENABLED", "false")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "true")
    settings = Settings()
    with pytest.raises(ValueError, match="authentication"):
        validate_api_http_security(settings)


def test_create_app_production_without_auth_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SECURITY_MODE", "production")
    monkeypatch.setenv("AUTH_ENABLED", "false")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "true")
    get_settings.cache_clear()
    with pytest.raises(ValueError, match="authentication"):
        create_app()
    get_settings.cache_clear()


def test_internal_process_starts_without_api_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SECURITY_MODE", "production")
    monkeypatch.setenv("AUTH_ENABLED", "false")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "false")
    monkeypatch.delenv("VIEWER_API_KEY", raising=False)
    monkeypatch.delenv("OPERATOR_API_KEY", raising=False)
    monkeypatch.delenv("VIEWER_API_KEY_FILE", raising=False)
    monkeypatch.delenv("OPERATOR_API_KEY_FILE", raising=False)
    settings = Settings()
    assert settings.effective_auth_enabled is False
    assert settings.resolved_viewer_api_key() is None
    assert settings.resolved_operator_api_key() is None


def test_production_rejects_missing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SECURITY_MODE", "production")
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("VIEWER_API_KEY", VIEWER)
    monkeypatch.delenv("OPERATOR_API_KEY", raising=False)
    with pytest.raises(ValidationError):
        Settings()


def test_production_rejects_weak_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SECURITY_MODE", "production")
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("VIEWER_API_KEY", "too-short")
    monkeypatch.setenv("OPERATOR_API_KEY", OPERATOR)
    with pytest.raises(ValidationError):
        Settings()


def test_identical_keys_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SECURITY_MODE", "production")
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("VIEWER_API_KEY", VIEWER)
    monkeypatch.setenv("OPERATOR_API_KEY", VIEWER)
    with pytest.raises(ValidationError):
        Settings()


def test_secret_file_preferred_over_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "viewer_api_key"
    path.write_text(VIEWER + "\n", encoding="utf-8")
    monkeypatch.setenv("SECURITY_MODE", "development")
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "false")
    monkeypatch.setenv("VIEWER_API_KEY", "z" * MIN_API_KEY_LENGTH)
    monkeypatch.setenv("VIEWER_API_KEY_FILE", str(path))
    monkeypatch.setenv("OPERATOR_API_KEY", OPERATOR)
    settings = Settings()
    assert settings.resolved_viewer_api_key() == VIEWER


def test_compare_digest_used_in_auth_path() -> None:
    text = Path("src/job_platform/security/auth.py").read_text(encoding="utf-8")
    assert "hmac.compare_digest" in text
    assert authenticate.__code__.co_consts is not None
