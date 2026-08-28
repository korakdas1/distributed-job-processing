from __future__ import annotations

from pathlib import Path

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from job_platform.api.app import create_app
from job_platform.core.config import MIN_API_KEY_LENGTH, get_settings
from job_platform.db.session import dispose_engine
from job_platform.queue.client import dispose_redis
from tests.integration.process_harness import ProcessCluster

pytestmark = pytest.mark.integration

VIEWER_KEY = "a" * MIN_API_KEY_LENGTH
OPERATOR_KEY = "b" * MIN_API_KEY_LENGTH
WORD_COUNT = {"job_type": "word_count", "payload": {"text": "hello world"}}


@pytest.fixture(autouse=True)
def _clear_secret_file_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VIEWER_API_KEY_FILE", raising=False)
    monkeypatch.delenv("OPERATOR_API_KEY_FILE", raising=False)


def _pg() -> psycopg.Connection:
    settings = get_settings()
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
    )


def _job_count() -> int:
    with _pg() as conn:
        row = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
    assert row is not None
    return int(row[0])


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def time(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@pytest.fixture
async def auth_client(redis_available: None, monkeypatch: pytest.MonkeyPatch) -> AsyncClient:
    monkeypatch.delenv("VIEWER_API_KEY_FILE", raising=False)
    monkeypatch.delenv("OPERATOR_API_KEY_FILE", raising=False)
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("DOCS_ENABLED", "true")
    monkeypatch.setenv("VIEWER_API_KEY", VIEWER_KEY)
    monkeypatch.setenv("OPERATOR_API_KEY", OPERATOR_KEY)
    get_settings.cache_clear()
    await dispose_engine()
    await dispose_redis()
    application = create_app()
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    await dispose_engine()
    await dispose_redis()
    get_settings.cache_clear()


def _viewer() -> dict[str, str]:
    return {"Authorization": f"Bearer {VIEWER_KEY}"}


def _operator() -> dict[str, str]:
    return {"Authorization": f"Bearer {OPERATOR_KEY}"}


async def test_missing_and_invalid_keys_are_401(auth_client: AsyncClient) -> None:
    missing = await auth_client.get("/jobs")
    assert missing.status_code == 401
    assert missing.json()["error"]["code"] == "AUTHENTICATION_REQUIRED"
    assert missing.headers.get("www-authenticate", "").lower().startswith("bearer")
    invalid = await auth_client.get("/jobs", headers={"Authorization": "Bearer not-a-valid-key"})
    assert invalid.status_code == 401
    assert invalid.json()["error"]["code"] == "INVALID_API_KEY"
    assert invalid.headers.get("www-authenticate", "").lower().startswith("bearer")


async def test_health_and_ready_remain_public(auth_client: AsyncClient) -> None:
    health = await auth_client.get("/health")
    assert health.status_code == 200
    ready = await auth_client.get("/ready")
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"


async def test_viewer_reads_and_cannot_write(auth_client: AsyncClient) -> None:
    listed = await auth_client.get("/jobs", headers=_viewer())
    assert listed.status_code == 200
    workers = await auth_client.get("/workers", headers=_viewer())
    assert workers.status_code == 200
    metrics = await auth_client.get("/metrics", headers=_viewer())
    assert metrics.status_code == 200
    summary = await auth_client.get("/metrics/summary", headers=_viewer())
    assert summary.status_code == 200
    before = _job_count()
    posted = await auth_client.post("/jobs", json=WORD_COUNT, headers=_viewer())
    assert posted.status_code == 403
    assert posted.json()["error"]["code"] == "INSUFFICIENT_PERMISSION"
    assert _job_count() == before


async def test_operator_can_submit_and_read(auth_client: AsyncClient) -> None:
    posted = await auth_client.post("/jobs", json=WORD_COUNT, headers=_operator())
    assert posted.status_code == 202
    job_id = posted.json()["id"]
    fetched = await auth_client.get(f"/jobs/{job_id}", headers=_operator())
    assert fetched.status_code == 200
    listed = await auth_client.get("/jobs", headers=_operator())
    assert listed.status_code == 200


async def test_metrics_require_viewer(auth_client: AsyncClient) -> None:
    assert (await auth_client.get("/metrics")).status_code == 401
    assert (await auth_client.get("/metrics/summary")).status_code == 401
    assert (await auth_client.get("/metrics", headers=_viewer())).status_code == 200
    assert (await auth_client.get("/metrics/summary", headers=_viewer())).status_code == 200


async def test_viewer_cannot_cancel_operator_can(
    auth_client: AsyncClient,
) -> None:
    posted = await auth_client.post("/jobs", json=WORD_COUNT, headers=_operator())
    job_id = posted.json()["id"]
    denied = await auth_client.delete(f"/jobs/{job_id}", headers=_viewer())
    assert denied.status_code == 403
    cancelled = await auth_client.delete(f"/jobs/{job_id}", headers=_operator())
    assert cancelled.status_code in {200, 202}
    assert cancelled.json()["status"] in {"CANCELLED", "RUNNING"}


async def test_idempotency_fingerprint_excludes_authorization(
    auth_client: AsyncClient,
) -> None:
    headers = {
        **_operator(),
        "Idempotency-Key": "security-idem-1",
    }
    first = await auth_client.post("/jobs", json=WORD_COUNT, headers=headers)
    second = await auth_client.post("/jobs", json=WORD_COUNT, headers=headers)
    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["id"] == second.json()["id"]
    assert second.headers.get("idempotency-replayed") == "true"


async def test_rate_limited_post_has_no_side_effect(
    redis_available: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("VIEWER_API_KEY", VIEWER_KEY)
    monkeypatch.setenv("OPERATOR_API_KEY", OPERATOR_KEY)
    monkeypatch.setenv("RATE_LIMIT_WRITE_PER_MINUTE", "60")
    monkeypatch.setenv("RATE_LIMIT_WRITE_BURST", "2")
    get_settings.cache_clear()
    await dispose_engine()
    await dispose_redis()
    clock = FakeClock()
    application = create_app(clock=clock.time)
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.post("/jobs", json=WORD_COUNT, headers=_operator())
        second = await client.post("/jobs", json=WORD_COUNT, headers=_operator())
        assert first.status_code == 202
        assert second.status_code == 202
        before = _job_count()
        limited = await client.post("/jobs", json=WORD_COUNT, headers=_operator())
        assert limited.status_code == 429
        assert limited.json()["error"]["code"] == "RATE_LIMIT_EXCEEDED"
        assert limited.headers.get("retry-after")
        assert _job_count() == before
        clock.advance(2.0)
        recovered = await client.post("/jobs", json=WORD_COUNT, headers=_operator())
        assert recovered.status_code == 202
    await dispose_engine()
    await dispose_redis()
    get_settings.cache_clear()


async def test_authenticated_redis_down_post_still_202(
    auth_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = str(get_settings().redis_port)
    monkeypatch.setenv("REDIS_PORT", "1")
    get_settings.cache_clear()
    await dispose_redis()
    posted = await auth_client.post("/jobs", json=WORD_COUNT, headers=_operator())
    assert posted.status_code == 202
    job_id = posted.json()["id"]
    ready = await auth_client.get("/ready")
    assert ready.status_code == 200
    assert ready.json()["degraded"] is True
    with _pg() as conn:
        event = conn.execute(
            "SELECT published_at FROM outbox_events WHERE job_id = %s",
            (job_id,),
        ).fetchone()
    assert event is not None
    assert event[0] is None
    monkeypatch.setenv("REDIS_PORT", original)
    get_settings.cache_clear()
    await dispose_redis()


async def test_authenticated_postgres_down_post_is_503(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("VIEWER_API_KEY", VIEWER_KEY)
    monkeypatch.setenv("OPERATOR_API_KEY", OPERATOR_KEY)
    monkeypatch.setenv("POSTGRES_PORT", "1")
    get_settings.cache_clear()
    await dispose_engine()
    await dispose_redis()
    application = create_app()
    async with AsyncClient(
        transport=ASGITransport(app=application),
        base_url="http://test",
    ) as isolated:
        posted = await isolated.post("/jobs", json=WORD_COUNT, headers=_operator())
        assert posted.status_code == 503
        assert posted.json()["error"]["code"] == "DATABASE_UNAVAILABLE"
    await dispose_engine()
    get_settings.cache_clear()
    await dispose_redis()


async def test_production_docs_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SECURITY_MODE", "production")
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("DOCS_ENABLED", "false")
    monkeypatch.setenv("VIEWER_API_KEY", VIEWER_KEY)
    monkeypatch.setenv("OPERATOR_API_KEY", OPERATOR_KEY)
    monkeypatch.setenv("ALLOWED_HOSTS", "test,localhost,127.0.0.1")
    get_settings.cache_clear()
    application = create_app()
    async with AsyncClient(
        transport=ASGITransport(app=application),
        base_url="http://test",
    ) as client:
        assert (await client.get("/docs")).status_code == 404
        assert (await client.get("/redoc")).status_code == 404
        assert (await client.get("/openapi.json")).status_code == 404
        health = await client.get("/health")
        assert health.status_code == 200
    get_settings.cache_clear()


async def test_trusted_host_rejects_invalid_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SECURITY_MODE", "production")
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("VIEWER_API_KEY", VIEWER_KEY)
    monkeypatch.setenv("OPERATOR_API_KEY", OPERATOR_KEY)
    monkeypatch.setenv("ALLOWED_HOSTS", "127.0.0.1,localhost")
    get_settings.cache_clear()
    application = create_app()
    async with AsyncClient(
        transport=ASGITransport(app=application),
        base_url="http://127.0.0.1",
    ) as client:
        ok = await client.get("/health", headers={"host": "127.0.0.1"})
        assert ok.status_code == 200
        bad = await client.get("/health", headers={"host": "evil.example"})
        assert bad.status_code == 400
    get_settings.cache_clear()


async def test_openapi_documents_bearer_api_key(auth_client: AsyncClient) -> None:
    response = await auth_client.get("/openapi.json")
    assert response.status_code == 200
    spec = response.json()
    schemes = spec["components"]["securitySchemes"]
    bearer = next(iter(schemes.values()))
    assert bearer["type"] == "http"
    assert bearer["scheme"] == "bearer"


async def test_development_client_still_open(client: AsyncClient) -> None:
    posted = await client.post("/jobs", json=WORD_COUNT)
    assert posted.status_code == 202
    listed = await client.get("/jobs")
    assert listed.status_code == 200


def test_internal_roles_start_without_api_keys(
    redis_available: None,
    tmp_path: Path,
) -> None:
    cluster = ProcessCluster(tmp_path)
    cluster.env["SECURITY_MODE"] = "production"
    cluster.env["AUTH_ENABLED"] = "false"
    cluster.env["RATE_LIMIT_ENABLED"] = "false"
    cluster.env.pop("VIEWER_API_KEY", None)
    cluster.env.pop("OPERATOR_API_KEY", None)
    cluster.env.pop("VIEWER_API_KEY_FILE", None)
    cluster.env.pop("OPERATOR_API_KEY_FILE", None)
    try:
        cluster.start_publisher()
        cluster.start_scheduler()
        cluster.start_workers(1)
        assert cluster.publisher_alive()
        assert cluster.scheduler_alive()
        assert cluster.worker_alive(0)
    finally:
        cluster.stop_all()
