"""Integration tests talk only to local `job_platform_test` and Redis DB 15."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import psycopg
import pytest
import redis as redis_sync
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient

os.environ["POSTGRES_HOST"] = "127.0.0.1"
os.environ["POSTGRES_DB"] = "job_platform_test"
os.environ["REDIS_HOST"] = "127.0.0.1"
os.environ["REDIS_DB"] = "15"
os.environ["RETRY_BASE_DELAY_SECONDS"] = "0.15"
os.environ["RETRY_MAX_DELAY_SECONDS"] = "0.5"
os.environ["RETRY_JITTER_RATIO"] = "0"
os.environ["RETRY_SCHEDULER_POLL_INTERVAL_MS"] = "50"
os.environ["WORKER_HEARTBEAT_INTERVAL_SECONDS"] = "0.2"
os.environ["WORKER_HEARTBEAT_TTL_SECONDS"] = "1.0"
os.environ["WORKER_DB_HEARTBEAT_INTERVAL_SECONDS"] = "0.4"
os.environ["SECURITY_MODE"] = "development"
os.environ["AUTH_ENABLED"] = "false"
os.environ["RATE_LIMIT_ENABLED"] = "false"
for _name in (
    "COMPOSE_PROJECT_NAME",
    "VIEWER_API_KEY",
    "VIEWER_API_KEY_FILE",
    "OPERATOR_API_KEY",
    "OPERATOR_API_KEY_FILE",
    "TLS_CERT_FILE",
    "TLS_KEY_FILE",
):
    os.environ.pop(_name, None)

from job_platform.core.config import get_settings  # noqa: E402
from job_platform.db.session import dispose_engine  # noqa: E402
from job_platform.queue.client import dispose_redis  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
TEST_DB = "job_platform_test"
TEST_REDIS_DB = 15
ALLOWED_HOSTS = frozenset({"127.0.0.1", "localhost"})


def _assert_safe_test_target() -> None:
    get_settings.cache_clear()
    settings = get_settings()
    if settings.postgres_host not in ALLOWED_HOSTS:
        pytest.fail("Refusing to run integration tests against a non-local database.")
    if settings.postgres_db != TEST_DB:
        pytest.fail("Refusing to run integration tests against a non-test database.")


def _assert_safe_redis_target() -> None:
    get_settings.cache_clear()
    settings = get_settings()
    if settings.redis_host not in ALLOWED_HOSTS:
        pytest.fail("Refusing to run Redis tests against a non-local host.")
    if settings.redis_db != TEST_REDIS_DB:
        pytest.fail("Refusing to flush a non-test Redis database.")


def _admin_connect() -> psycopg.Connection:
    settings = get_settings()
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname="postgres",
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        autocommit=True,
    )


def _test_connect() -> psycopg.Connection:
    settings = get_settings()
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=TEST_DB,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
    )


def _sync_redis() -> redis_sync.Redis:
    settings = get_settings()
    return redis_sync.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
        socket_connect_timeout=2,
    )


def _flush_test_redis() -> None:
    _assert_safe_redis_target()
    client = _sync_redis()
    try:
        client.flushdb()
    finally:
        client.close()


@pytest.fixture(scope="session")
def test_database() -> Iterator[None]:
    _assert_safe_test_target()
    with _admin_connect() as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (TEST_DB,),
        ).fetchone()
        if exists is None:
            conn.execute(f'CREATE DATABASE "{TEST_DB}"')
    cfg = Config(str(ROOT / "alembic.ini"))
    command.upgrade(cfg, "head")
    yield


@pytest.fixture(scope="session")
def redis_available(test_database: None) -> Iterator[None]:
    _assert_safe_redis_target()
    client = _sync_redis()
    try:
        client.ping()
    except Exception:
        pytest.fail(
            "Redis is required for these integration tests. "
            "Start it with: docker-compose up -d redis"
        )
    finally:
        client.close()
    yield


@pytest.fixture(autouse=True)
async def isolate_stores(redis_available: None) -> AsyncIterator[None]:
    _assert_safe_test_target()
    _assert_safe_redis_target()
    await dispose_engine()
    await dispose_redis()
    with _test_connect() as conn:
        conn.execute("SET lock_timeout = '5s'")
        conn.execute("TRUNCATE TABLE jobs CASCADE")
        conn.execute("TRUNCATE TABLE workers")
        conn.commit()
    _flush_test_redis()
    yield
    await dispose_engine()
    await dispose_redis()


@pytest.fixture
async def client(redis_available: None) -> AsyncIterator[AsyncClient]:
    get_settings.cache_clear()
    await dispose_engine()
    await dispose_redis()
    from job_platform.api.app import create_app

    application = create_app()
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client
    await dispose_engine()
    await dispose_redis()
