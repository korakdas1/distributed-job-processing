from __future__ import annotations

import logging
import time
import uuid

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import SQLAlchemyError

from job_platform.api.app import create_app
from job_platform.core.config import get_settings
from job_platform.core.logging import log_event
from job_platform.db.session import dispose_engine
from job_platform.queue.client import dispose_redis
from job_platform.queue.streams import (
    ensure_consumer_groups,
    pending_count,
    publish_job_id,
    read_one,
)
from job_platform.worker.processor import process_message
from tests.integration.helpers import (
    TEST_WORKER_ID,
    consumer_pending,
    process_next_message,
    stream_entries,
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


async def test_word_count_end_to_end(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "hello distributed world"}},
    )
    assert created.status_code == 202
    job_id = created.json()["id"]
    assert created.json()["status"] == "QUEUED"

    message = await process_next_message()
    assert message is not None
    assert str(message.job_id) == job_id

    fetched = await client.get(f"/jobs/{job_id}")
    body = fetched.json()
    assert body["status"] == "SUCCEEDED"
    assert body["result"] == {"word_count": 3}
    assert body["attempt_count"] == 1
    assert body["worker_id"] is None
    assert body["completed_at"] is not None
    assert body["error"] is None

    with _pg() as conn:
        attempt = conn.execute(
            """
            SELECT attempt_number, status, worker_id, finished_at, duration_ms, error,
                   delivery_message_id
            FROM job_attempts WHERE job_id = %s
            """,
            (job_id,),
        ).fetchone()
        attempts = conn.execute(
            "SELECT COUNT(*) FROM job_attempts WHERE job_id = %s",
            (job_id,),
        ).fetchone()
    assert attempts is not None
    assert attempts[0] == 1
    assert attempt is not None
    assert attempt[0] == 1
    assert attempt[1] == "SUCCEEDED"
    assert attempt[2] == TEST_WORKER_ID
    assert attempt[3] is not None
    assert attempt[4] is not None
    assert attempt[4] >= 0
    assert attempt[5] is None
    assert attempt[6]
    assert await consumer_pending() == 0


@pytest.mark.parametrize(
    ("body", "expected_result"),
    [
        (
            {"job_type": "word_count", "payload": {"text": "one two"}},
            {"word_count": 2},
        ),
        (
            {"job_type": "sum_numbers", "payload": {"numbers": [1, 2, 3.5]}},
            {"sum": 6.5},
        ),
        (
            {"job_type": "sleep", "payload": {"seconds": 0.05}},
            {"slept_seconds": 0.05},
        ),
        (
            {"job_type": "prime_calculation", "payload": {"limit": 100}},
            {"count": 25, "largest_prime": 97},
        ),
    ],
)
async def test_successful_handlers_end_to_end(
    client: AsyncClient,
    body: dict[str, object],
    expected_result: dict[str, object],
) -> None:
    created = await client.post("/jobs", json=body)
    assert created.status_code == 202
    job_id = created.json()["id"]
    await process_next_message()
    fetched = await client.get(f"/jobs/{job_id}")
    payload = fetched.json()
    assert payload["status"] == "SUCCEEDED"
    assert payload["result"] == expected_result
    assert payload["attempt_count"] == 1
    assert payload["worker_id"] is None


async def test_simulate_failure_is_terminal_without_retry(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "simulate_failure", "payload": {}},
    )
    job_id = created.json()["id"]
    await process_next_message()
    fetched = await client.get(f"/jobs/{job_id}")
    body = fetched.json()
    assert body["status"] == "FAILED"
    assert body["attempt_count"] == 1
    assert body["result"] is None
    assert body["error"]["code"] == "SIMULATED_FAILURE"
    assert body["error"]["retryable"] is False
    assert body["completed_at"] is not None
    assert body["worker_id"] is None

    with _pg() as conn:
        rows = conn.execute(
            "SELECT attempt_number, status, error FROM job_attempts WHERE job_id = %s",
            (job_id,),
        ).fetchall()
        status = conn.execute(
            "SELECT status FROM jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
    assert status is not None
    assert status[0] == "FAILED"
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert rows[0][1] == "FAILED"
    assert rows[0][2]["code"] == "SIMULATED_FAILURE"
    assert await consumer_pending() == 0


async def test_duplicate_terminal_message_is_acked_without_rerun(
    client: AsyncClient,
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "once only"}},
    )
    job_id = uuid.UUID(created.json()["id"])
    await process_next_message()
    assert (await client.get(f"/jobs/{job_id}")).json()["status"] == "SUCCEEDED"

    await publish_job_id(job_id, stream=get_settings().redis_stream_normal)
    await process_next_message()

    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "SUCCEEDED"
    assert fetched.json()["attempt_count"] == 1
    with _pg() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM job_attempts WHERE job_id = %s",
            (str(job_id),),
        ).fetchone()
    assert count is not None
    assert count[0] == 1
    assert await consumer_pending() == 0


async def test_orphan_message_is_acked_without_creating_job(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = uuid.uuid4()
    await ensure_consumer_groups()
    await publish_job_id(missing, stream=get_settings().redis_stream_normal)
    assert len(await stream_entries()) == 1

    events: list[str] = []
    warnings: list[str] = []

    def _capture_event(logger: logging.Logger, event: str, **fields: object) -> None:
        events.append(event)
        log_event(logger, event, **fields)

    def _capture_warning(msg: object, *args: object, **kwargs: object) -> None:
        warnings.append(str(msg))

    monkeypatch.setattr("job_platform.worker.processor.log_event", _capture_event)
    monkeypatch.setattr(
        "job_platform.worker.processor.logger.warning",
        _capture_warning,
    )
    message = await process_next_message()
    assert message is not None
    assert message.job_id == missing
    listed = await client.get("/jobs")
    assert listed.json()["total"] == 0
    with _pg() as conn:
        row = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
        attempts = conn.execute("SELECT COUNT(*) FROM job_attempts").fetchone()
    assert row is not None and row[0] == 0
    assert attempts is not None and attempts[0] == 0
    assert await consumer_pending() == 0
    assert "orphan_message" in events
    assert any("no PostgreSQL row" in item for item in warnings)


async def test_malformed_stream_message_is_poison_acked() -> None:
    await ensure_consumer_groups()
    settings = get_settings()
    from job_platform.queue.client import get_redis

    await get_redis().xadd(settings.redis_stream_normal, {"job_id": "not-a-uuid"})
    message = await read_one(TEST_WORKER_ID, block_ms=2000)
    assert message is not None
    assert message.job_id is None
    await process_message(TEST_WORKER_ID, message)
    assert await pending_count() == 0


async def test_worker_starts_after_job_is_already_queued(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "buffered"}},
    )
    job_id = created.json()["id"]
    queued = await client.get(f"/jobs/{job_id}")
    assert queued.json()["status"] == "QUEUED"
    assert await stream_entries() == []

    await process_next_message()
    done = await client.get(f"/jobs/{job_id}")
    assert done.json()["status"] == "SUCCEEDED"
    assert done.json()["result"] == {"word_count": 1}


async def test_api_restart_does_not_lose_queued_work(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "sum_numbers", "payload": {"numbers": [4, 6]}},
    )
    job_id = created.json()["id"]

    await dispose_engine()
    await dispose_redis()
    application = create_app()
    async with AsyncClient(
        transport=ASGITransport(app=application),
        base_url="http://test",
    ) as restarted:
        still_queued = await restarted.get(f"/jobs/{job_id}")
        assert still_queued.json()["status"] == "QUEUED"

    await dispose_engine()
    await dispose_redis()
    await process_next_message()
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "SUCCEEDED"
    assert fetched.json()["result"] == {"sum": 10.0}


async def test_succeeded_result_survives_worker_restart(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "durable result"}},
    )
    job_id = created.json()["id"]
    await process_next_message("worker-first01")
    first = (await client.get(f"/jobs/{job_id}")).json()
    await process_next_message("worker-second2")
    second = (await client.get(f"/jobs/{job_id}")).json()
    assert first["status"] == "SUCCEEDED"
    assert second["result"] == first["result"] == {"word_count": 2}
    assert second["attempt_count"] == 1


async def test_post_sleep_returns_before_handler_finishes(client: AsyncClient) -> None:
    started = time.perf_counter()
    created = await client.post(
        "/jobs",
        json={"job_type": "sleep", "payload": {"seconds": 2}},
    )
    elapsed = time.perf_counter() - started
    assert created.status_code == 202
    assert elapsed < 1.0
    job_id = created.json()["id"]
    queued = await client.get(f"/jobs/{job_id}")
    assert queued.json()["status"] == "QUEUED"
    await process_next_message()
    done = await client.get(f"/jobs/{job_id}")
    assert done.json()["status"] == "SUCCEEDED"
    assert done.json()["result"] == {"slept_seconds": 2.0}


async def test_persist_failure_after_execution_leaves_pending(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "pending"}},
    )
    job_id = created.json()["id"]

    async def boom(*_args: object, **_kwargs: object) -> None:
        raise SQLAlchemyError("simulated persist failure")

    monkeypatch.setattr(
        "job_platform.worker.processor.persist_terminal_outcome",
        boom,
    )
    await process_next_message()
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "RUNNING"
    assert await consumer_pending() == 1


async def test_unexpected_running_status_is_not_acked(client: AsyncClient) -> None:
    created = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "already running"}},
    )
    job_id = created.json()["id"]
    with _pg() as conn:
        conn.execute("UPDATE jobs SET status = 'RUNNING' WHERE id = %s", (job_id,))
        conn.commit()
    await process_next_message()
    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.json()["status"] == "RUNNING"
    assert fetched.json()["attempt_count"] == 0
    assert await consumer_pending() == 1
