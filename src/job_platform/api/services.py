"""Job persistence with a transactional outbox. The API does not talk to Redis."""

from __future__ import annotations

import logging
import math
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from job_platform.core.cancellation import apply_terminal_cancellation
from job_platform.core.clock import utcnow
from job_platform.core.config import Settings
from job_platform.core.enums import JobPriority, JobStatus, JobType
from job_platform.core.errors import AppError
from job_platform.core.logging import log_event
from job_platform.idempotency.hashing import (
    canonical_request_fingerprint,
    hash_idempotency_key,
    validate_idempotency_key,
)
from job_platform.models.idempotency import SubmissionIdempotency
from job_platform.models.job import Job
from job_platform.models.outbox import OutboxEvent
from job_platform.outbox.events import (
    create_dispatch_outbox_event,
    create_initial_schedule_outbox_event,
)
from job_platform.schemas.job import JobCreateRequest
from job_platform.tasks.definitions import (
    assert_payload_size,
    parse_job_type,
    validate_payload,
)

logger = logging.getLogger(__name__)

IDEMPOTENCY_PK = "pk_submission_idempotency"


@dataclass(frozen=True)
class JobSubmitResult:
    job: Job
    replayed: bool


def _idempotency_uniqueness_error(exc: IntegrityError) -> bool:
    current: BaseException | None = exc.orig
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        constraint = getattr(getattr(current, "diag", None), "constraint_name", None)
        if constraint == IDEMPOTENCY_PK:
            return True
        current = current.__cause__ or getattr(current, "__context__", None)
    return IDEMPOTENCY_PK in str(exc)


def create_job_record(
    *,
    settings: Settings,
    job_type: str,
    payload: dict[str, Any],
    priority: JobPriority,
    max_attempts: int | None,
    timeout_seconds: int | None,
    delay_seconds: float,
) -> Job:
    if not math.isfinite(delay_seconds) or delay_seconds < 0:
        raise AppError(
            "VALIDATION_ERROR",
            "delay_seconds must be a finite number greater than or equal to 0.",
            status_code=422,
            details={"delay_seconds": delay_seconds},
        )
    if delay_seconds > settings.max_initial_delay_seconds:
        raise AppError(
            "DELAY_TOO_LARGE",
            "delay_seconds exceeds the maximum initial delay.",
            status_code=422,
            details={
                "delay_seconds": delay_seconds,
                "max_initial_delay_seconds": settings.max_initial_delay_seconds,
            },
        )

    parsed_type = parse_job_type(job_type)
    canonical_payload = validate_payload(parsed_type, payload)
    assert_payload_size(canonical_payload, settings.payload_max_bytes)

    now = utcnow()
    delayed = delay_seconds > 0
    run_after = now + timedelta(seconds=delay_seconds) if delayed else None
    return Job(
        id=uuid.uuid4(),
        job_type=parsed_type.value,
        payload=canonical_payload,
        status=JobStatus.SCHEDULED.value if delayed else JobStatus.QUEUED.value,
        priority=priority.value,
        result=None,
        error=None,
        attempt_count=0,
        max_attempts=max_attempts if max_attempts is not None else settings.retry_max_attempts,
        created_at=now,
        queued_at=None if delayed else now,
        started_at=None,
        completed_at=None,
        cancel_requested_at=None,
        cancelled_at=None,
        next_retry_at=None,
        run_after=run_after,
        worker_id=None,
        idempotency_key=None,
        timeout_seconds=(
            timeout_seconds if timeout_seconds is not None else settings.job_lease_timeout_seconds
        ),
    )


async def persist_job_with_outbox(session: AsyncSession, job: Job) -> tuple[Job, OutboxEvent]:
    """INSERT job + outbox row in one transaction. No Redis."""
    event = _attach_initial_outbox(job)
    session.add(job)
    session.add(event)
    await session.flush()
    await session.commit()
    log_event(
        logger,
        "outbox_event_created",
        job_id=job.id,
        event_id=event.id,
        event_type=event.event_type,
    )
    return job, event


def _attach_initial_outbox(job: Job) -> OutboxEvent:
    if job.status == JobStatus.SCHEDULED.value:
        if job.run_after is None:
            raise AppError(
                "INTERNAL_ERROR",
                "SCHEDULED jobs require run_after.",
                status_code=500,
            )
        event = create_initial_schedule_outbox_event(job, run_at=job.run_after)
        log_event(
            logger,
            "scheduled_job_created",
            job_id=job.id,
            priority=job.priority,
            run_after=job.run_after.isoformat(),
        )
    else:
        event = create_dispatch_outbox_event(job)
    return event


async def _get_idempotency_row(
    session: AsyncSession, key_hash: str
) -> SubmissionIdempotency | None:
    return await session.get(SubmissionIdempotency, key_hash)


async def _replay_or_conflict(
    session: AsyncSession,
    row: SubmissionIdempotency,
    fingerprint: str,
) -> Job:
    if row.request_fingerprint != fingerprint:
        raise AppError(
            "IDEMPOTENCY_KEY_CONFLICT",
            "The Idempotency-Key was already used for a different job submission.",
            status_code=409,
        )
    job = await get_job_by_id(session, row.job_id)
    if job is None:
        raise AppError(
            "INTERNAL_ERROR",
            "Idempotency record is missing its job.",
            status_code=500,
        )
    return job


async def submit_job(
    session: AsyncSession,
    *,
    settings: Settings,
    body: JobCreateRequest,
    idempotency_key: str | None,
) -> JobSubmitResult:
    job = create_job_record(
        settings=settings,
        job_type=body.job_type,
        payload=body.payload,
        priority=body.priority,
        max_attempts=body.max_attempts,
        timeout_seconds=body.timeout_seconds,
        delay_seconds=body.delay_seconds,
    )
    if idempotency_key is None:
        stored, _event = await persist_job_with_outbox(session, job)
        return JobSubmitResult(job=stored, replayed=False)

    validate_idempotency_key(idempotency_key)
    key_hash = hash_idempotency_key(idempotency_key)
    fingerprint = canonical_request_fingerprint(body, settings)
    existing = await _get_idempotency_row(session, key_hash)
    if existing is not None:
        replayed = await _replay_or_conflict(session, existing, fingerprint)
        return JobSubmitResult(job=replayed, replayed=True)

    event = _attach_initial_outbox(job)
    record = SubmissionIdempotency(
        key_hash=key_hash,
        request_fingerprint=fingerprint,
        job_id=job.id,
        created_at=job.created_at,
    )
    session.add(job)
    session.add(event)
    await session.flush()
    session.add(record)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        if not _idempotency_uniqueness_error(exc):
            raise
        winner = await _get_idempotency_row(session, key_hash)
        if winner is None:
            raise AppError(
                "INTERNAL_ERROR",
                "Idempotency race resolved without a durable record.",
                status_code=500,
            ) from exc
        replayed = await _replay_or_conflict(session, winner, fingerprint)
        return JobSubmitResult(job=replayed, replayed=True)
    log_event(
        logger,
        "outbox_event_created",
        job_id=job.id,
        event_id=event.id,
        event_type=event.event_type,
        key_hash_prefix=key_hash[:8],
    )
    return JobSubmitResult(job=job, replayed=False)


async def request_job_cancellation(session: AsyncSession, job_id: uuid.UUID) -> tuple[Job, int]:
    """Cancel a job. Waiting jobs become CANCELLED; RUNNING records a request.

    Does not write to Redis. Stale stream/ZSET members are cleaned by workers
    and the scheduler after they observe durable PostgreSQL state.
    """
    stmt = select(Job).where(Job.id == job_id).with_for_update()
    job = await session.scalar(stmt)
    if job is None:
        raise AppError(
            "JOB_NOT_FOUND",
            "No job exists with the given id.",
            status_code=404,
            details={"job_id": str(job_id)},
        )
    status = JobStatus(job.status)
    now = utcnow()
    if status is JobStatus.CANCELLED:
        await session.commit()
        return job, 200
    if status in {JobStatus.SUCCEEDED, JobStatus.FAILED}:
        raise AppError(
            "JOB_NOT_CANCELLABLE",
            "A completed job cannot be cancelled.",
            status_code=409,
            details={"status": status.value},
        )
    if status is JobStatus.RUNNING:
        if job.cancel_requested_at is None:
            job.cancel_requested_at = now
        await session.commit()
        return job, 202
    if status in {JobStatus.SCHEDULED, JobStatus.QUEUED, JobStatus.RETRYING}:
        apply_terminal_cancellation(job, now=now)
        await session.commit()
        return job, 200
    raise AppError(
        "JOB_NOT_CANCELLABLE",
        "A completed job cannot be cancelled.",
        status_code=409,
        details={"status": status.value},
    )


async def get_job_by_id(session: AsyncSession, job_id: uuid.UUID) -> Job | None:
    return await session.get(Job, job_id)


def _list_filters(
    *,
    status: JobStatus | None,
    job_type: JobType | None,
    priority: JobPriority | None,
    created_after: datetime | None,
    created_before: datetime | None,
) -> list[Any]:
    filters: list[Any] = []
    if status is not None:
        filters.append(Job.status == status.value)
    if job_type is not None:
        filters.append(Job.job_type == job_type.value)
    if priority is not None:
        filters.append(Job.priority == priority.value)
    if created_after is not None:
        filters.append(Job.created_at >= created_after)
    if created_before is not None:
        filters.append(Job.created_at <= created_before)
    return filters


def _filtered_select(
    *,
    status: JobStatus | None,
    job_type: JobType | None,
    priority: JobPriority | None,
    created_after: datetime | None,
    created_before: datetime | None,
) -> Select[tuple[Job]]:
    stmt = select(Job)
    for clause in _list_filters(
        status=status,
        job_type=job_type,
        priority=priority,
        created_after=created_after,
        created_before=created_before,
    ):
        stmt = stmt.where(clause)
    return stmt


async def list_jobs(
    session: AsyncSession,
    *,
    limit: int,
    offset: int,
    status: JobStatus | None = None,
    job_type: JobType | None = None,
    priority: JobPriority | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
) -> tuple[Sequence[Job], int]:
    clauses = _list_filters(
        status=status,
        job_type=job_type,
        priority=priority,
        created_after=created_after,
        created_before=created_before,
    )
    count_stmt = select(func.count()).select_from(Job)
    for clause in clauses:
        count_stmt = count_stmt.where(clause)
    total = int(await session.scalar(count_stmt) or 0)

    stmt = (
        _filtered_select(
            status=status,
            job_type=job_type,
            priority=priority,
            created_after=created_after,
            created_before=created_before,
        )
        .order_by(Job.created_at.desc(), Job.id.desc())
        .limit(limit)
        .offset(offset)
    )
    rows = (await session.scalars(stmt)).all()
    return rows, total
