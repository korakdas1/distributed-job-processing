"""Promote due SCHEDULED and RETRYING jobs. PostgreSQL is authoritative.

Does not XADD ready work. Promotion creates JOB_DISPATCH outbox, then ZREM.
Malformed delayed members are removed. One scheduler handles first-run delay
and retry backoff.
"""

from __future__ import annotations

import logging
import uuid

from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from job_platform.core.clock import utcnow
from job_platform.core.config import get_settings
from job_platform.core.enums import JobStatus
from job_platform.core.logging import log_event
from job_platform.core.transitions import TERMINAL_STATUSES, assert_transition
from job_platform.db.session import get_session_factory
from job_platform.models.job import Job
from job_platform.outbox.events import create_dispatch_outbox_event
from job_platform.queue.delayed import (
    DelayedMember,
    due_members,
    remove_delayed,
    remove_delayed_member,
    rescore_delayed,
)

logger = logging.getLogger(__name__)


async def _lock_job(session: AsyncSession, job_id: uuid.UUID) -> Job | None:
    stmt = select(Job).where(Job.id == job_id).with_for_update()
    result = await session.scalar(stmt)
    return result if isinstance(result, Job) else None


async def _promote_queued(session: AsyncSession, job: Job) -> uuid.UUID:
    now = utcnow()
    status = JobStatus(job.status)
    assert_transition(status, JobStatus.QUEUED)
    job.status = JobStatus.QUEUED.value
    job.queued_at = now
    job.worker_id = None
    if status is JobStatus.RETRYING:
        job.next_retry_at = None
    event = create_dispatch_outbox_event(job)
    session.add(event)
    await session.commit()
    return event.id


async def _promote_one(member: DelayedMember) -> None:
    job_id = member.job_id
    assert job_id is not None
    now = utcnow()
    factory = get_session_factory()
    async with factory() as session:
        job = await _lock_job(session, job_id)
        if job is None:
            await session.rollback()
            log_event(logger, "retry_delayed_orphan", job_id=job_id)
            await remove_delayed(member)
            return
        status = JobStatus(job.status)
        if status in TERMINAL_STATUSES:
            await session.rollback()
            log_event(
                logger,
                "retry_delayed_stale_removed",
                job_id=job_id,
                status=status.value,
            )
            await remove_delayed(member)
            return
        if status in {JobStatus.QUEUED, JobStatus.RUNNING}:
            await session.rollback()
            log_event(
                logger,
                "retry_delayed_stale_removed",
                job_id=job_id,
                status=status.value,
            )
            await remove_delayed(member)
            return
        if status is JobStatus.SCHEDULED:
            if job.run_after is None:
                await session.rollback()
                log_event(
                    logger,
                    "scheduled_missing_run_after",
                    job_id=job_id,
                )
                logger.error("SCHEDULED job is missing run_after; refusing to guess a time")
                return
            if job.run_after > now:
                run_after = job.run_after
                await session.rollback()
                await rescore_delayed(member, run_after)
                log_event(
                    logger,
                    "scheduled_rescored",
                    job_id=job_id,
                    run_after=run_after.isoformat(),
                )
                return
            log_event(
                logger,
                "scheduled_job_due",
                job_id=job_id,
                run_after=job.run_after.isoformat(),
            )
            event_id = await _promote_queued(session, job)
            log_event(
                logger,
                "scheduled_job_promoted",
                job_id=job_id,
                event_id=event_id,
            )
        elif status is JobStatus.RETRYING:
            if job.next_retry_at is None:
                await session.rollback()
                log_event(
                    logger,
                    "retrying_missing_next_retry_at",
                    job_id=job_id,
                )
                logger.error("RETRYING job is missing next_retry_at; refusing to guess a time")
                return
            if job.next_retry_at > now:
                next_retry_at = job.next_retry_at
                await session.rollback()
                await rescore_delayed(member, next_retry_at)
                log_event(
                    logger,
                    "retry_rescored",
                    job_id=job_id,
                    next_retry_at=next_retry_at.isoformat(),
                )
                return
            log_event(
                logger,
                "retry_due",
                job_id=job_id,
                next_retry_at=job.next_retry_at.isoformat(),
            )
            event_id = await _promote_queued(session, job)
            log_event(
                logger,
                "retry_promoted",
                job_id=job_id,
                event_id=event_id,
            )
        else:
            await session.rollback()
            log_event(
                logger,
                "retry_delayed_skipped",
                job_id=job_id,
                status=status.value,
            )
            return
    try:
        await remove_delayed(member)
    except RedisError:
        log_event(logger, "retry_zrem_failed", job_id=job_id)
        logger.exception("ZREM after delayed promotion failed; later pass will drop stale member")


async def promote_due_jobs(*, limit: int | None = None) -> int:
    settings = get_settings()
    batch = settings.retry_scheduler_batch_size if limit is None else limit
    now = utcnow()
    due = await due_members(now, batch)
    handled = 0
    for member in due:
        if member.job_id is None:
            log_event(
                logger,
                "malformed_delayed_member_removed",
                member=member.raw,
            )
            logger.warning("Removing malformed jobs:delayed member %r", member.raw)
            await remove_delayed_member(member)
            handled += 1
            continue
        await _promote_one(member)
        handled += 1
    return handled
