"""Publish unpublished outbox events to Redis. At-least-once, not exactly-once.

JOB_DISPATCH -> XADD the ready stream for the durable Job.priority
JOB_INITIAL_SCHEDULE -> ZADD jobs:delayed (redis_message_id stays NULL)
JOB_RETRY_SCHEDULE -> ZADD jobs:delayed (redis_message_id stays NULL)
JOB_DEAD_LETTER -> XADD jobs:dead (redis_message_id is the stream ID)

Unknown event types and unknown priorities are never marked published.
Old JOB_DISPATCH rows with payload={} still route from PostgreSQL Job.priority.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any

from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from job_platform.core.clock import utcnow
from job_platform.core.config import get_settings
from job_platform.core.enums import JobStatus
from job_platform.core.logging import log_event
from job_platform.db.session import get_session_factory
from job_platform.models.job import Job
from job_platform.models.outbox import (
    OUTBOX_EVENT_JOB_DEAD_LETTER,
    OUTBOX_EVENT_JOB_DISPATCH,
    OUTBOX_EVENT_JOB_INITIAL_SCHEDULE,
    OUTBOX_EVENT_JOB_RETRY_SCHEDULE,
    OutboxEvent,
)
from job_platform.queue.dead_letter import publish_dead_letter
from job_platform.queue.delayed import schedule_delayed
from job_platform.queue.priority import UnknownPriorityError, stream_for_priority
from job_platform.queue.streams import publish_job_id

logger = logging.getLogger(__name__)

_LAST_ERROR_MAX = 500


class UnknownOutboxEventTypeError(RuntimeError):
    """Poison outbox row: event_type is not a known publisher action."""


class OutboxRoutingError(RuntimeError):
    """Durable job data cannot be routed (missing job or invalid priority)."""


def _safe_error_text(exc: BaseException) -> str:
    summary = f"{type(exc).__name__}: {exc}"
    return summary[:_LAST_ERROR_MAX]


def _payload(event: OutboxEvent) -> dict[str, Any]:
    raw = event.payload
    return dict(raw) if isinstance(raw, dict) else {}


def _run_at_from_payload(payload: dict[str, Any], *, event_type: str) -> datetime:
    raw = payload.get("run_at")
    if not isinstance(raw, str) or raw == "":
        msg = f"{event_type} payload is missing run_at"
        raise ValueError(msg)
    return datetime.fromisoformat(raw)


async def _dispatch_ready(session: AsyncSession, event: OutboxEvent) -> str:
    job = await session.get(Job, event.job_id)
    if job is None:
        raise OutboxRoutingError(f"JOB_DISPATCH {event.id} references missing job {event.job_id}")
    try:
        stream = stream_for_priority(job.priority)
    except UnknownPriorityError as exc:
        raise OutboxRoutingError(str(exc)) from exc
    message_id = await publish_job_id(event.job_id, stream=stream, outbox_event_id=event.id)
    log_event(
        logger,
        "priority_dispatch",
        event_id=event.id,
        job_id=event.job_id,
        priority=job.priority,
        stream=stream,
        message_id=message_id,
    )
    return message_id


async def _publish_schedule(session: AsyncSession, event: OutboxEvent) -> None:
    payload = _payload(event)
    run_at = _run_at_from_payload(payload, event_type=event.event_type)
    # Serialize publication with durable round transitions. A retry outbox can
    # be replayed after its Redis write but after a later attempt has committed.
    # UUID identity alone prevents stale cleanup, not stale publication.
    job = await session.scalar(select(Job).where(Job.id == event.job_id).with_for_update())
    if job is None:
        raise OutboxRoutingError(f"Schedule {event.id} references missing job {event.job_id}")
    initial = event.event_type == OUTBOX_EVENT_JOB_INITIAL_SCHEDULE
    current = (
        job.status == JobStatus.SCHEDULED.value
        and job.attempt_count == 0
        and job.run_after == run_at
        if initial
        else job.status == JobStatus.RETRYING.value
        and job.attempt_count == payload.get("failed_attempt")
        and job.next_retry_at == run_at
    )
    if not current:
        log_event(logger, "schedule_event_obsolete", event_id=event.id, job_id=event.job_id)
        return
    await schedule_delayed(event.job_id, run_at, outbox_event_id=event.id)
    log_event(
        logger,
        "initial_schedule_published" if initial else "retry_schedule_published",
        event_id=event.id,
        job_id=event.job_id,
        run_at=run_at.isoformat(),
    )


async def _route_event(session: AsyncSession, event: OutboxEvent) -> str | None:
    if event.event_type == OUTBOX_EVENT_JOB_DISPATCH:
        return await _dispatch_ready(session, event)
    if event.event_type in {OUTBOX_EVENT_JOB_INITIAL_SCHEDULE, OUTBOX_EVENT_JOB_RETRY_SCHEDULE}:
        await _publish_schedule(session, event)
        return None
    if event.event_type == OUTBOX_EVENT_JOB_DEAD_LETTER:
        payload = _payload(event)
        reason = str(payload.get("reason") or "UNKNOWN")
        attempt_count = int(payload.get("attempt_count") or 0)
        error_code = payload.get("error_code")
        error_code_str = str(error_code) if error_code is not None else None
        return await publish_dead_letter(
            event.job_id,
            outbox_event_id=event.id,
            reason=reason,
            attempt_count=attempt_count,
            error_code=error_code_str,
        )
    raise UnknownOutboxEventTypeError(
        f"Unknown outbox event_type {event.event_type!r} for event {event.id}"
    )


async def _lock_unpublished(session: AsyncSession, *, limit: int) -> list[OutboxEvent]:
    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.created_at.asc(), OutboxEvent.id.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    rows = (await session.scalars(stmt)).all()
    return list(rows)


async def _publish_one(session: AsyncSession, event: OutboxEvent) -> None:
    event.publish_attempts += 1
    try:
        message_id = await _route_event(session, event)
    except RedisError as exc:
        event.last_error = _safe_error_text(exc)
        await session.commit()
        log_event(
            logger,
            "outbox_publish_failed",
            event_id=event.id,
            job_id=event.job_id,
            event_type=event.event_type,
        )
        raise
    except (UnknownOutboxEventTypeError, OutboxRoutingError) as exc:
        event.last_error = _safe_error_text(exc)
        await session.commit()
        log_name = (
            "outbox_unknown_event_type"
            if isinstance(exc, UnknownOutboxEventTypeError)
            else "outbox_routing_error"
        )
        log_event(
            logger,
            log_name,
            event_id=event.id,
            job_id=event.job_id,
            event_type=event.event_type,
        )
        raise
    event.published_at = utcnow()
    event.redis_message_id = message_id
    event.last_error = None
    await session.commit()
    log_event(
        logger,
        "outbox_event_published",
        event_id=event.id,
        job_id=event.job_id,
        event_type=event.event_type,
        redis_message_id=message_id,
    )


async def publish_available(*, limit: int | None = None) -> int:
    """Publish a small batch of unpublished events in created_at order.

    Returns how many events were marked published this call.
    """
    settings = get_settings()
    batch = settings.outbox_batch_size if limit is None else limit
    factory = get_session_factory()
    published = 0
    async with factory() as session:
        events = await _lock_unpublished(session, limit=batch)
        if not events:
            await session.rollback()
            return 0
        event_ids = [event.id for event in events]
        await session.rollback()

    for event_id in event_ids:
        async with factory() as session:
            event = await session.get(OutboxEvent, event_id, with_for_update=True)
            if event is None or event.published_at is not None:
                await session.rollback()
                continue
            await _publish_one(session, event)
            published += 1
    return published


async def publish_event_ids(event_ids: list[uuid.UUID]) -> int:
    """Test helper: publish specific unpublished events in the given order."""
    factory = get_session_factory()
    published = 0
    for event_id in event_ids:
        async with factory() as session:
            event = await session.get(OutboxEvent, event_id, with_for_update=True)
            if event is None or event.published_at is not None:
                await session.rollback()
                continue
            await _publish_one(session, event)
            published += 1
    return published
