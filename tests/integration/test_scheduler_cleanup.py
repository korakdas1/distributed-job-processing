"""Deterministic delayed-cleanup interleavings; no service-control helpers."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from job_platform.core.config import get_settings
from job_platform.db.session import get_session_factory
from job_platform.models.job import Job, JobAttempt
from job_platform.models.outbox import OUTBOX_EVENT_JOB_DISPATCH, OutboxEvent
from job_platform.queue.client import get_redis
from job_platform.queue.delayed import (
    due_members,
    remove_delayed,
    rescore_delayed,
    schedule_delayed,
)
from job_platform.scheduler import promoter
from job_platform.worker import processor
from tests.integration.helpers import delayed_members, drain_outbox, process_next_message

pytestmark = pytest.mark.integration


async def retrying_job(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> tuple[str, datetime]:
    now = datetime(2030, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(processor, "utcnow", lambda: now)
    created = await client.post(
        "/jobs",
        json={
            "job_type": "simulate_failure",
            "payload": {"mode": "retryable"},
            "max_attempts": 3,
            "priority": "HIGH",
        },
    )
    assert created.status_code == 202
    job_id = created.json()["id"]
    assert await process_next_message() is not None
    await drain_outbox()
    body = (await client.get(f"/jobs/{job_id}")).json()
    assert body["status"] == "RETRYING"
    due = datetime.fromisoformat(body["next_retry_at"])
    monkeypatch.setattr(promoter, "utcnow", lambda: due + timedelta(microseconds=1))
    return job_id, due


@pytest.mark.parametrize("equal_score", [False, True])
@pytest.mark.parametrize("legacy", [False, True])
async def test_new_retry_survives_old_promotion_cleanup(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch, equal_score: bool, legacy: bool
) -> None:
    """Pause after the actual PostgreSQL commit, then run/publish attempt two."""
    job_id, due = await retrying_job(client, monkeypatch)
    if legacy:
        await get_redis().hdel(f"{get_settings().redis_delayed_zset}:generations", job_id)
    committed = asyncio.Event()
    resume = asyncio.Event()
    real_promote = promoter._promote_queued

    async def pause_after_commit(session, job):
        event_id = await real_promote(session, job)
        committed.set()
        await resume.wait()
        return event_id

    monkeypatch.setattr(promoter, "_promote_queued", pause_after_commit)
    old_scheduler = asyncio.create_task(promoter.promote_due_jobs())
    try:
        await asyncio.wait_for(committed.wait(), timeout=5)
        # Attempt two's configured backoff is 0.3 seconds. Equal-score replacement
        # deliberately models clock overlap; wall-clock uniqueness is not assumed.
        worker_now = due - timedelta(seconds=0.3) if equal_score else due + timedelta(seconds=1)
        monkeypatch.setattr(processor, "utcnow", lambda: worker_now)
        assert await process_next_message() is not None
        await drain_outbox()
        body = (await client.get(f"/jobs/{job_id}")).json()
        assert body["status"] == "RETRYING"
        assert body["attempt_count"] == 2
        newer_due = datetime.fromisoformat(body["next_retry_at"])
        assert (newer_due == due) is equal_score
        assert (job_id, newer_due.timestamp()) in await delayed_members()
    finally:
        resume.set()
        await asyncio.wait_for(old_scheduler, timeout=5)

    assert (job_id, newer_due.timestamp()) in await delayed_members(), (
        "old scheduler cleanup deleted the newer retry schedule"
    )
    monkeypatch.setattr(promoter, "_promote_queued", real_promote)
    monkeypatch.setattr(promoter, "utcnow", lambda: newer_due + timedelta(microseconds=1))
    await promoter.promote_due_jobs()
    assert await process_next_message() is not None
    final = (await client.get(f"/jobs/{job_id}")).json()
    assert final["status"] == "FAILED"
    assert final["attempt_count"] == 3
    assert final["error"]["code"] == "MAX_ATTEMPTS_EXHAUSTED"


async def durable_counts(job_id: str) -> tuple[int, int]:
    async with get_session_factory()() as session:
        dispatches = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .where(
                OutboxEvent.job_id == uuid.UUID(job_id),
                OutboxEvent.event_type == OUTBOX_EVENT_JOB_DISPATCH,
            )
        )
        attempts = await session.scalar(
            select(func.count())
            .select_from(JobAttempt)
            .where(JobAttempt.job_id == uuid.UUID(job_id))
        )
    return int(dispatches or 0), int(attempts or 0)


@pytest.mark.parametrize("running", [False, True])
@pytest.mark.parametrize("equal_score", [False, True])
async def test_stale_status_cleanup_preserves_new_retry(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch, running: bool, equal_score: bool
) -> None:
    job_id, due = await retrying_job(client, monkeypatch)

    async def leave_old_member(_member):
        return False

    monkeypatch.setattr(promoter, "remove_delayed", leave_old_member)
    await promoter.promote_due_jobs()
    monkeypatch.setattr(promoter, "remove_delayed", remove_delayed)
    handler_started = asyncio.Event()
    finish_handler = asyncio.Event()
    real_handler = processor.execute_handler

    async def blocked_handler(*args):
        handler_started.set()
        await finish_handler.wait()
        return await real_handler(*args)

    worker = None
    if running:
        monkeypatch.setattr(processor, "execute_handler", blocked_handler)
        worker = asyncio.create_task(process_next_message())
        await asyncio.wait_for(handler_started.wait(), 5)
    observed = asyncio.Event()
    resume = asyncio.Event()
    real_rollback = AsyncSession.rollback
    old_scheduler = None

    async def pause_after_rollback(session):
        await real_rollback(session)
        if asyncio.current_task() is old_scheduler:
            observed.set()
            await resume.wait()

    monkeypatch.setattr(AsyncSession, "rollback", pause_after_rollback)
    old_scheduler = asyncio.create_task(promoter.promote_due_jobs())
    try:
        await asyncio.wait_for(observed.wait(), 5)
        body = (await client.get(f"/jobs/{job_id}")).json()
        assert body["status"] == ("RUNNING" if running else "QUEUED")
        worker_now = due - timedelta(seconds=0.3) if equal_score else due + timedelta(seconds=1)
        monkeypatch.setattr(processor, "utcnow", lambda: worker_now)
        finish_handler.set()
        if worker is None:
            assert await process_next_message() is not None
        else:
            assert await asyncio.wait_for(worker, 5) is not None
        await drain_outbox()
        body = (await client.get(f"/jobs/{job_id}")).json()
        assert body["status"] == "RETRYING"
        newer_due = datetime.fromisoformat(body["next_retry_at"])
        assert (newer_due == due) is equal_score
    finally:
        finish_handler.set()
        resume.set()
        await asyncio.wait_for(old_scheduler, 5)
        if worker is not None:
            await asyncio.wait_for(worker, 5)
    assert (job_id, newer_due.timestamp()) in await delayed_members()
    assert await durable_counts(job_id) == (2, 2)


async def test_two_observations_dispatch_once(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    job_id, _due = await retrying_job(client, monkeypatch)
    observed = asyncio.Event()
    real_due = promoter.due_members
    readers = 0

    async def both_observe(*args):
        nonlocal readers
        result = await real_due(*args)
        assert len(result) == 1
        readers += 1
        if readers == 2:
            observed.set()
        await asyncio.wait_for(observed.wait(), 5)
        return result

    monkeypatch.setattr(promoter, "due_members", both_observe)
    await asyncio.wait_for(
        asyncio.gather(promoter.promote_due_jobs(), promoter.promote_due_jobs()), 5
    )
    assert await durable_counts(job_id) == (2, 1)
    assert await delayed_members() == []
    assert (await client.get(f"/jobs/{job_id}")).json()["status"] == "QUEUED"


async def test_failed_promotion_commit_keeps_schedule(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    job_id, due = await retrying_job(client, monkeypatch)
    real_commit = AsyncSession.commit

    async def failed_commit(session):
        if any(
            isinstance(row, OutboxEvent) and row.event_type == OUTBOX_EVENT_JOB_DISPATCH
            for row in session.new
        ):
            raise SQLAlchemyError("injected promotion commit failure")
        await real_commit(session)

    monkeypatch.setattr(AsyncSession, "commit", failed_commit)
    with pytest.raises(SQLAlchemyError, match="injected promotion commit failure"):
        await promoter.promote_due_jobs()
    assert (job_id, due.timestamp()) in await delayed_members()
    assert (await client.get(f"/jobs/{job_id}")).json()["status"] == "RETRYING"
    assert await durable_counts(job_id) == (1, 1)
    monkeypatch.setattr(AsyncSession, "commit", real_commit)
    await promoter.promote_due_jobs()
    assert await durable_counts(job_id) == (2, 1)


@pytest.mark.parametrize("equal_score", [False, True])
async def test_old_rescore_cannot_overwrite_next_retry(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch, equal_score: bool
) -> None:
    job_id, due = await retrying_job(client, monkeypatch)
    stale_due = due - timedelta(seconds=2)
    await get_redis().zadd(get_settings().redis_delayed_zset, {job_id: stale_due.timestamp()})
    monkeypatch.setattr(promoter, "utcnow", lambda: due - timedelta(seconds=1))
    observed = asyncio.Event()
    resume = asyncio.Event()

    async def paused_rescore(*args):
        observed.set()
        await resume.wait()
        return await rescore_delayed(*args)

    monkeypatch.setattr(promoter, "rescore_delayed", paused_rescore)
    old_scheduler = asyncio.create_task(promoter.promote_due_jobs())
    try:
        await asyncio.wait_for(observed.wait(), 5)
        monkeypatch.setattr(promoter, "utcnow", lambda: due + timedelta(microseconds=1))
        await promoter.promote_due_jobs()
        worker_now = (
            stale_due - timedelta(seconds=0.3) if equal_score else due + timedelta(seconds=1)
        )
        monkeypatch.setattr(processor, "utcnow", lambda: worker_now)
        assert await process_next_message() is not None
        await drain_outbox()
        body = (await client.get(f"/jobs/{job_id}")).json()
        newer_due = datetime.fromisoformat(body["next_retry_at"])
        assert (newer_due == stale_due) is equal_score
    finally:
        resume.set()
        await asyncio.wait_for(old_scheduler, 5)
    assert (job_id, newer_due.timestamp()) in await delayed_members()


@pytest.mark.parametrize("legacy", [False, True])
async def test_atomic_mutations_missing_member_and_score_round_trip(legacy: bool) -> None:
    job_id = uuid.uuid4()
    # Redis's decimal representation and Python's repr need not be identical.
    due = datetime(2030, 1, 1, 0, 0, 0, 123457, tzinfo=UTC)
    key = get_settings().redis_delayed_zset
    if legacy:
        await get_redis().zadd(key, {str(job_id): due.timestamp()})
    else:
        await schedule_delayed(job_id, due, outbox_event_id=uuid.uuid4())
    (member,) = await due_members(due, 10)
    assert member.score == due.timestamp()
    assert (member.generation is None) is legacy
    assert await rescore_delayed(member, due + timedelta(seconds=1))
    assert not await remove_delayed(member)
    (rescored,) = await due_members(due + timedelta(seconds=1), 10)
    assert await remove_delayed(rescored)
    assert not await remove_delayed(rescored)
    assert not await rescore_delayed(rescored, due)
    assert await get_redis().zscore(key, str(job_id)) is None
    assert await get_redis().hget(f"{key}:generations", str(job_id)) is None


@pytest.mark.parametrize("equal_score", [False, True])
@pytest.mark.parametrize("initial", [False, True])
async def test_old_publication_cannot_replace_new_round(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch, equal_score: bool, initial: bool
) -> None:
    if initial:
        created = await client.post(
            "/jobs",
            json={
                "job_type": "simulate_failure",
                "payload": {"mode": "retryable"},
                "delay_seconds": 1,
                "max_attempts": 3,
            },
        )
        job_id = created.json()["id"]
        due = datetime.fromisoformat(created.json()["run_after"])
        await drain_outbox()
        monkeypatch.setattr(promoter, "utcnow", lambda: due + timedelta(microseconds=1))
    else:
        job_id, due = await retrying_job(client, monkeypatch)
    (old_member,) = await due_members(due, 10)
    assert old_member.generation is not None
    # Duplicate publication before promotion keeps exactly the same identity.
    async with get_session_factory()() as session:
        old = await session.get(OutboxEvent, uuid.UUID(old_member.generation))
        assert old is not None
        old.published_at = None
        await session.commit()
    await drain_outbox()
    assert await due_members(due, 10) == [old_member]
    await promoter.promote_due_jobs()
    # A publication replay after consumption must not resurrect the old round.
    async with get_session_factory()() as session:
        old = await session.get(OutboxEvent, uuid.UUID(old_member.generation))
        assert old is not None
        old.published_at = None
        await session.commit()
    await drain_outbox()
    assert await delayed_members() == []
    backoff = 0.15 if initial else 0.3
    worker_now = due - timedelta(seconds=backoff) if equal_score else due + timedelta(seconds=1)
    monkeypatch.setattr(processor, "utcnow", lambda: worker_now)
    assert await process_next_message() is not None
    await drain_outbox()
    (newer,) = await due_members(due + timedelta(seconds=10), 10)
    assert newer.generation != old_member.generation
    assert (newer.score == old_member.score) is equal_score
    async with get_session_factory()() as session:
        old = await session.get(OutboxEvent, uuid.UUID(old_member.generation))
        assert old is not None
        old.published_at = None
        await session.commit()
    assert await drain_outbox() == 1
    assert await due_members(due + timedelta(seconds=10), 10) == [newer]
    assert not await remove_delayed(old_member)
    assert not await rescore_delayed(old_member, due - timedelta(seconds=10))
    async with get_session_factory()() as session:
        job = await session.get(Job, uuid.UUID(job_id))
        assert job is not None and job.status == "RETRYING"
        assert job.attempt_count == (1 if initial else 2)


async def test_publication_holds_job_lock_until_commit(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = await client.post(
        "/jobs", json={"job_type": "word_count", "payload": {"text": "later"}, "delay_seconds": 1}
    )
    job_id = uuid.UUID(created.json()["id"])
    publishing = asyncio.Event()
    resume = asyncio.Event()

    async def paused_publish(*args, **kwargs):
        publishing.set()
        await resume.wait()
        await schedule_delayed(*args, **kwargs)

    monkeypatch.setattr("job_platform.outbox.publisher.schedule_delayed", paused_publish)
    publisher = asyncio.create_task(drain_outbox())
    try:
        await asyncio.wait_for(publishing.wait(), 5)
        async with get_session_factory()() as session:
            with pytest.raises(SQLAlchemyError) as exc:
                await session.scalar(
                    select(Job).where(Job.id == job_id).with_for_update(nowait=True)
                )
            assert exc.value.orig.sqlstate == "55P03"  # PostgreSQL lock_not_available
    finally:
        resume.set()
        await asyncio.wait_for(publisher, 5)
    async with get_session_factory()() as session:
        assert (
            await session.scalar(select(Job).where(Job.id == job_id).with_for_update(nowait=True))
            is not None
        )
