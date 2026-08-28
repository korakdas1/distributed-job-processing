"""HTTP Idempotency-Key: replay, conflict, races, Redis-down, scheduled run_after."""

from __future__ import annotations

import asyncio

import psycopg
import pytest
from httpx import AsyncClient
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from job_platform.core.config import get_settings
from job_platform.db.session import dispose_engine
from job_platform.idempotency.hashing import hash_idempotency_key
from job_platform.queue.client import dispose_redis

pytestmark = pytest.mark.integration

WORD_COUNT = {"job_type": "word_count", "payload": {"text": "hello world"}}


def _pg() -> psycopg.Connection:
    settings = get_settings()
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
    )


def _counts(job_id: str) -> tuple[int, int, int]:
    with _pg() as conn:
        jobs = conn.execute("SELECT COUNT(*) FROM jobs WHERE id = %s", (job_id,)).fetchone()
        events = conn.execute(
            "SELECT COUNT(*) FROM outbox_events WHERE job_id = %s", (job_id,)
        ).fetchone()
        keys = conn.execute(
            "SELECT COUNT(*) FROM submission_idempotency WHERE job_id = %s", (job_id,)
        ).fetchone()
    assert jobs is not None and events is not None and keys is not None
    return int(jobs[0]), int(events[0]), int(keys[0])


async def test_sequential_replay_same_key(client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "idem-replay-a"}
    first = await client.post("/jobs", json=WORD_COUNT, headers=headers)
    assert first.status_code == 202
    assert "Idempotency-Replayed" not in first.headers
    job_id = first.json()["id"]
    assert first.headers["location"] == f"/jobs/{job_id}"
    second = await client.post("/jobs", json=WORD_COUNT, headers=headers)
    assert second.status_code == 202
    assert second.json()["id"] == job_id
    assert second.headers["Idempotency-Replayed"] == "true"
    assert second.headers["location"] == f"/jobs/{job_id}"
    assert _counts(job_id) == (1, 1, 1)
    print(f"IDEMPOTENCY_REPLAY job={job_id}")


async def test_raw_key_is_not_stored(client: AsyncClient) -> None:
    raw = "VERY_SECRET_IDEMPOTENCY_KEY_12345"
    created = await client.post("/jobs", json=WORD_COUNT, headers={"Idempotency-Key": raw})
    job_id = created.json()["id"]
    digest = hash_idempotency_key(raw)
    with _pg() as conn:
        row = conn.execute(
            "SELECT key_hash, request_fingerprint FROM submission_idempotency WHERE job_id = %s",
            (job_id,),
        ).fetchone()
        stored_key = conn.execute(
            "SELECT idempotency_key FROM jobs WHERE id = %s", (job_id,)
        ).fetchone()
        blob = conn.execute(
            "SELECT CAST(key_hash AS text) || CAST(request_fingerprint AS text) "
            "FROM submission_idempotency WHERE job_id = %s",
            (job_id,),
        ).fetchone()
    assert row is not None
    assert row[0] == digest
    assert raw not in row[0]
    assert stored_key is not None
    assert stored_key[0] is None
    assert blob is not None
    assert raw not in blob[0]


async def test_same_key_different_body_conflicts(client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "idem-conflict"}
    first = await client.post("/jobs", json=WORD_COUNT, headers=headers)
    job_id = first.json()["id"]
    conflict = await client.post(
        "/jobs",
        json={"job_type": "sum_numbers", "payload": {"numbers": [1, 2]}},
        headers=headers,
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_KEY_CONFLICT"
    assert "X-Request-ID" in conflict.headers
    listed = await client.get("/jobs")
    assert listed.json()["total"] == 1
    assert listed.json()["items"][0]["id"] == job_id
    print(f"IDEMPOTENCY_CONFLICT job={job_id}")


async def test_concurrent_same_key_creates_one_job(client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "idem-concurrent"}
    responses = await asyncio.gather(
        *[client.post("/jobs", json=WORD_COUNT, headers=headers) for _ in range(10)]
    )
    assert all(item.status_code == 202 for item in responses)
    ids = {item.json()["id"] for item in responses}
    assert len(ids) == 1
    job_id = next(iter(ids))
    with _pg() as conn:
        jobs = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
        events = conn.execute("SELECT COUNT(*) FROM outbox_events").fetchone()
        keys = conn.execute("SELECT COUNT(*) FROM submission_idempotency").fetchone()
    assert jobs is not None and events is not None and keys is not None
    assert int(jobs[0]) == 1
    assert int(events[0]) == 1
    assert int(keys[0]) == 1
    print(f"IDEMPOTENCY_CONCURRENT job={job_id} requests=10")


async def test_different_keys_create_different_jobs(client: AsyncClient) -> None:
    left = await client.post("/jobs", json=WORD_COUNT, headers={"Idempotency-Key": "key-a"})
    right = await client.post("/jobs", json=WORD_COUNT, headers={"Idempotency-Key": "key-b"})
    assert left.json()["id"] != right.json()["id"]
    listed = await client.get("/jobs")
    assert listed.json()["total"] == 2


async def test_no_key_still_creates_two_jobs(client: AsyncClient) -> None:
    first = await client.post("/jobs", json=WORD_COUNT)
    second = await client.post("/jobs", json=WORD_COUNT)
    assert first.json()["id"] != second.json()["id"]


async def test_scheduled_replay_preserves_run_after(client: AsyncClient) -> None:
    headers = {"Idempotency-Key": "idem-scheduled"}
    first = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "later"}, "delay_seconds": 10},
        headers=headers,
    )
    body = first.json()
    run_after = body["run_after"]
    await asyncio.sleep(0.2)
    second = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "later"}, "delay_seconds": 10},
        headers=headers,
    )
    assert second.json()["id"] == body["id"]
    assert second.json()["run_after"] == run_after
    assert second.headers["Idempotency-Replayed"] == "true"


async def test_replay_after_success(client: AsyncClient) -> None:
    from tests.integration.helpers import process_next_message

    headers = {"Idempotency-Key": "idem-after-success"}
    created = await client.post("/jobs", json=WORD_COUNT, headers=headers)
    job_id = created.json()["id"]
    await process_next_message()
    done = (await client.get(f"/jobs/{job_id}")).json()
    assert done["status"] == "SUCCEEDED"
    replay = await client.post("/jobs", json=WORD_COUNT, headers=headers)
    assert replay.json()["id"] == job_id
    assert replay.json()["status"] == "SUCCEEDED"


async def test_commit_failure_rolls_back_keyed_submission(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}
    real = AsyncSession.commit

    async def flaky_commit(self: AsyncSession) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise SQLAlchemyError("injected commit failure")
        return await real(self)

    monkeypatch.setattr(AsyncSession, "commit", flaky_commit)
    headers = {"Idempotency-Key": "idem-tx-fail"}
    failed = await client.post("/jobs", json=WORD_COUNT, headers=headers)
    assert failed.status_code == 503
    with _pg() as conn:
        jobs = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
        events = conn.execute("SELECT COUNT(*) FROM outbox_events").fetchone()
        keys = conn.execute("SELECT COUNT(*) FROM submission_idempotency").fetchone()
    assert jobs is not None and events is not None and keys is not None
    assert int(jobs[0]) == 0
    assert int(events[0]) == 0
    assert int(keys[0]) == 0
    monkeypatch.undo()
    retry = await client.post("/jobs", json=WORD_COUNT, headers=headers)
    assert retry.status_code == 202


async def test_idempotency_redis_down(client: AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    original = str(get_settings().redis_port)
    monkeypatch.setenv("REDIS_PORT", "1")
    get_settings.cache_clear()
    await dispose_engine()
    await dispose_redis()
    headers = {"Idempotency-Key": "idem-redis-down"}
    first = await client.post("/jobs", json=WORD_COUNT, headers=headers)
    assert first.status_code == 202
    job_id = first.json()["id"]
    second = await client.post("/jobs", json=WORD_COUNT, headers=headers)
    assert second.status_code == 202
    assert second.json()["id"] == job_id
    with _pg() as conn:
        unpublished = conn.execute(
            "SELECT COUNT(*) FROM outbox_events WHERE job_id = %s AND published_at IS NULL",
            (job_id,),
        ).fetchone()
        keys = conn.execute(
            "SELECT COUNT(*) FROM submission_idempotency WHERE job_id = %s", (job_id,)
        ).fetchone()
    assert unpublished is not None and int(unpublished[0]) == 1
    assert keys is not None and int(keys[0]) == 1
    monkeypatch.setenv("REDIS_PORT", original)
    get_settings.cache_clear()
    await dispose_redis()
    print(f"IDEMPOTENCY_REDIS_DOWN job={job_id}")
