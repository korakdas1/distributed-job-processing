"""Claim, execute, persist, then XACK. One job at a time.

Handler retryable failures become RETRYING + JOB_RETRY_SCHEDULE (outbox), not
immediate re-dispatch. Crash recovery remains lease reclaim + INTERRUPTED and
consumes the same max_attempts budget. Persist before XACK.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from job_platform.core.cancellation import apply_terminal_cancellation
from job_platform.core.clock import utcnow
from job_platform.core.config import get_settings
from job_platform.core.enums import AttemptStatus, JobStatus, JobType
from job_platform.core.logging import log_event
from job_platform.core.transitions import TERMINAL_STATUSES, assert_transition
from job_platform.db.session import get_session_factory
from job_platform.models.job import Job, JobAttempt
from job_platform.outbox.events import (
    DEAD_LETTER_BUDGET_AFTER_INTERRUPT,
    DEAD_LETTER_MAX_ATTEMPTS,
    DEAD_LETTER_NON_RETRYABLE,
    create_dead_letter_outbox_event,
    create_retry_schedule_outbox_event,
)
from job_platform.queue.streams import StreamMessage, ack_message
from job_platform.retry.classify import classify_handler_exception
from job_platform.retry.policy import RetryDecision, compute_retry_delay, decide_retry
from job_platform.tasks.context import TaskExecutionContext
from job_platform.tasks.errors import TaskCancelledError, TaskExecutionError
from job_platform.tasks.handlers import execute_handler

logger = logging.getLogger(__name__)

_LEASE_EXPIRED_ERROR = {
    "code": "WORKER_LEASE_EXPIRED",
    "message": "The previous worker did not complete before the processing lease expired.",
    "retryable": True,
}

_MAX_ATTEMPTS_JOB_ERROR = {
    "code": "MAX_ATTEMPTS_EXHAUSTED",
    "message": "The execution-attempt budget was exhausted.",
    "retryable": False,
}

_BUDGET_AFTER_INTERRUPT_ERROR = {
    "code": "ATTEMPT_BUDGET_EXHAUSTED_AFTER_INTERRUPT",
    "message": "The execution-attempt budget was exhausted after a worker interrupt.",
    "retryable": False,
    "last_error_code": "WORKER_LEASE_EXPIRED",
}

_CANCELLED_ATTEMPT_ERROR = {
    "code": "CANCELLED_BY_REQUEST",
    "message": "Execution stopped after a cancellation request.",
}


class WorkerOwnershipError(Exception):
    """Terminal persist refused because this worker no longer owns the execution."""


class AttemptBudgetExhaustedError(RuntimeError):
    """Caller tried to create attempt max_attempts + 1."""


@dataclass(frozen=True)
class ClaimedWork:
    job_id: uuid.UUID
    job_type: JobType
    payload: dict[str, Any]
    attempt_id: uuid.UUID
    attempt_number: int
    max_attempts: int
    delivery_stream: str
    delivery_message_id: str


def assert_terminal_ownership(
    job: Job,
    attempt: JobAttempt,
    *,
    worker_id: str,
    attempt_id: uuid.UUID,
    attempt_number: int,
    delivery_stream: str,
    delivery_message_id: str,
) -> None:
    """Refuse stale terminal writes. Does not mutate job or attempt."""
    problems: list[str] = []
    if JobStatus(job.status) is not JobStatus.RUNNING:
        problems.append(f"job.status={job.status}")
    if job.worker_id != worker_id:
        problems.append(f"job.worker_id={job.worker_id}")
    if job.attempt_count != attempt_number:
        problems.append(f"job.attempt_count={job.attempt_count}")
    if attempt.id != attempt_id:
        problems.append(f"attempt.id={attempt.id}")
    if attempt.job_id != job.id:
        problems.append(f"attempt.job_id={attempt.job_id}")
    if attempt.worker_id != worker_id:
        problems.append(f"attempt.worker_id={attempt.worker_id}")
    if AttemptStatus(attempt.status) is not AttemptStatus.RUNNING:
        problems.append(f"attempt.status={attempt.status}")
    if attempt.delivery_stream != delivery_stream:
        problems.append(f"attempt.delivery_stream={attempt.delivery_stream}")
    if attempt.delivery_message_id != delivery_message_id:
        problems.append(f"attempt.delivery_message_id={attempt.delivery_message_id}")
    if problems:
        raise WorkerOwnershipError("Worker does not own this execution: " + ", ".join(problems))


def attempt_budget_remaining(job: Job) -> bool:
    return job.attempt_count < job.max_attempts


async def _lock_job(session: AsyncSession, job_id: uuid.UUID) -> Job | None:
    stmt = select(Job).where(Job.id == job_id).with_for_update()
    result = await session.scalar(stmt)
    return result if isinstance(result, Job) else None


async def _lock_attempt(session: AsyncSession, attempt_id: uuid.UUID) -> JobAttempt | None:
    stmt = select(JobAttempt).where(JobAttempt.id == attempt_id).with_for_update()
    result = await session.scalar(stmt)
    return result if isinstance(result, JobAttempt) else None


async def _lock_active_attempt(session: AsyncSession, job: Job) -> JobAttempt | None:
    stmt = (
        select(JobAttempt)
        .where(
            JobAttempt.job_id == job.id,
            JobAttempt.attempt_number == job.attempt_count,
        )
        .with_for_update()
    )
    result = await session.scalar(stmt)
    return result if isinstance(result, JobAttempt) else None


async def claim_queued_job(
    session: AsyncSession,
    job: Job,
    worker_id: str,
    *,
    delivery_stream: str,
    delivery_message_id: str,
) -> JobAttempt:
    if not attempt_budget_remaining(job):
        raise AttemptBudgetExhaustedError(
            f"Refusing attempt {job.attempt_count + 1} beyond max_attempts={job.max_attempts}"
        )
    assert_transition(JobStatus(job.status), JobStatus.RUNNING)
    now = utcnow()
    attempt_number = job.attempt_count + 1
    job.status = JobStatus.RUNNING.value
    job.attempt_count = attempt_number
    job.started_at = now
    job.worker_id = worker_id
    attempt = JobAttempt(
        id=uuid.uuid4(),
        job_id=job.id,
        attempt_number=attempt_number,
        worker_id=worker_id,
        started_at=now,
        status=AttemptStatus.RUNNING.value,
        delivery_stream=delivery_stream,
        delivery_message_id=delivery_message_id,
    )
    session.add(attempt)
    await session.flush()
    return attempt


def _interrupt_attempt(attempt: JobAttempt) -> None:
    now = utcnow()
    elapsed_ms = max(0, int((now - attempt.started_at).total_seconds() * 1000))
    attempt.status = AttemptStatus.INTERRUPTED.value
    attempt.finished_at = now
    attempt.error = dict(_LEASE_EXPIRED_ERROR)
    attempt.duration_ms = elapsed_ms


async def recover_crashed_attempt(
    session: AsyncSession,
    job: Job,
    attempt: JobAttempt,
    worker_id: str,
    *,
    delivery_stream: str,
    delivery_message_id: str,
) -> JobAttempt:
    """RUNNING -> INTERRUPTED, then QUEUED -> RUNNING as a new attempt. One transaction."""
    _interrupt_attempt(attempt)
    log_event(
        logger,
        "attempt_interrupted",
        job_id=job.id,
        worker_id=attempt.worker_id,
        attempt=attempt.attempt_number,
        stream=delivery_stream,
        message_id=delivery_message_id,
    )
    assert_transition(JobStatus(job.status), JobStatus.QUEUED)
    job.status = JobStatus.QUEUED.value
    job.worker_id = None
    new_attempt = await claim_queued_job(
        session,
        job,
        worker_id,
        delivery_stream=delivery_stream,
        delivery_message_id=delivery_message_id,
    )
    log_event(
        logger,
        "job_recovered",
        job_id=job.id,
        worker_id=worker_id,
        attempt=new_attempt.attempt_number,
        stream=delivery_stream,
        message_id=delivery_message_id,
    )
    return new_attempt


async def fail_after_interrupt_budget_exhausted(
    session: AsyncSession,
    job: Job,
    attempt: JobAttempt,
    *,
    delivery_stream: str,
    delivery_message_id: str,
) -> None:
    """No attempt N+1. Interrupt the active attempt and dead-letter the job."""
    _interrupt_attempt(attempt)
    log_event(
        logger,
        "attempt_interrupted",
        job_id=job.id,
        worker_id=attempt.worker_id,
        attempt=attempt.attempt_number,
        stream=delivery_stream,
        message_id=delivery_message_id,
    )
    assert_transition(JobStatus(job.status), JobStatus.FAILED)
    now = utcnow()
    job.status = JobStatus.FAILED.value
    job.worker_id = None
    job.completed_at = now
    job.next_retry_at = None
    job.result = None
    job.error = dict(_BUDGET_AFTER_INTERRUPT_ERROR)
    session.add(
        create_dead_letter_outbox_event(
            job,
            reason=DEAD_LETTER_BUDGET_AFTER_INTERRUPT,
            error_code="WORKER_LEASE_EXPIRED",
            attempt_count=job.attempt_count,
        )
    )
    log_event(
        logger,
        "max_attempts_exhausted",
        job_id=job.id,
        attempt=job.attempt_count,
        reason=DEAD_LETTER_BUDGET_AFTER_INTERRUPT,
    )
    log_event(
        logger,
        "dead_letter_scheduled",
        job_id=job.id,
        reason=DEAD_LETTER_BUDGET_AFTER_INTERRUPT,
        attempt=job.attempt_count,
    )


async def fail_queued_budget_exhausted(session: AsyncSession, job: Job) -> None:
    assert_transition(JobStatus(job.status), JobStatus.FAILED)
    now = utcnow()
    job.status = JobStatus.FAILED.value
    job.worker_id = None
    job.completed_at = now
    job.next_retry_at = None
    job.result = None
    job.error = dict(_MAX_ATTEMPTS_JOB_ERROR)
    session.add(
        create_dead_letter_outbox_event(
            job,
            reason=DEAD_LETTER_MAX_ATTEMPTS,
            error_code="MAX_ATTEMPTS_EXHAUSTED",
            attempt_count=job.attempt_count,
        )
    )
    log_event(
        logger,
        "max_attempts_exhausted",
        job_id=job.id,
        attempt=job.attempt_count,
        reason=DEAD_LETTER_MAX_ATTEMPTS,
    )
    log_event(
        logger,
        "dead_letter_scheduled",
        job_id=job.id,
        reason=DEAD_LETTER_MAX_ATTEMPTS,
        attempt=job.attempt_count,
    )


async def persist_terminal_outcome(
    session: AsyncSession,
    *,
    job_id: uuid.UUID,
    attempt_id: uuid.UUID,
    attempt_number: int,
    worker_id: str,
    delivery_stream: str,
    delivery_message_id: str,
    success: bool,
    result: dict[str, Any] | None,
    error: dict[str, Any] | None,
    duration_ms: int,
) -> None:
    job = await _lock_job(session, job_id)
    if job is None:
        raise RuntimeError(f"Job {job_id} disappeared during persist")
    attempt = await _lock_attempt(session, attempt_id)
    if attempt is None:
        raise RuntimeError(f"Attempt {attempt_id} missing for job {job_id}")
    assert_terminal_ownership(
        job,
        attempt,
        worker_id=worker_id,
        attempt_id=attempt_id,
        attempt_number=attempt_number,
        delivery_stream=delivery_stream,
        delivery_message_id=delivery_message_id,
    )
    if job.cancel_requested_at is not None:
        _finalize_owned_cancellation(job, attempt, duration_ms=duration_ms)
        return
    if not success:
        await persist_handler_failure(
            session,
            job=job,
            attempt=attempt,
            worker_id=worker_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            delivery_stream=delivery_stream,
            delivery_message_id=delivery_message_id,
            error=error
            or {
                "code": "TASK_EXECUTION_FAILED",
                "message": "Task execution failed.",
                "retryable": False,
            },
            duration_ms=duration_ms,
        )
        return
    assert_transition(JobStatus(job.status), JobStatus.SUCCEEDED)
    now = utcnow()
    job.status = JobStatus.SUCCEEDED.value
    job.result = result
    job.error = None
    job.completed_at = now
    job.worker_id = None
    job.next_retry_at = None

    attempt.status = AttemptStatus.SUCCEEDED.value
    attempt.finished_at = now
    attempt.error = None
    attempt.duration_ms = duration_ms


async def persist_handler_failure(
    session: AsyncSession,
    *,
    job: Job | None = None,
    attempt: JobAttempt | None = None,
    job_id: uuid.UUID | None = None,
    attempt_id: uuid.UUID | None = None,
    attempt_number: int,
    worker_id: str,
    delivery_stream: str,
    delivery_message_id: str,
    error: dict[str, Any],
    duration_ms: int,
) -> None:
    if job is None:
        if job_id is None:
            raise RuntimeError("persist_handler_failure requires job or job_id")
        job = await _lock_job(session, job_id)
        if job is None:
            raise RuntimeError(f"Job {job_id} disappeared during persist")
    if attempt is None:
        if attempt_id is None:
            raise RuntimeError("persist_handler_failure requires attempt or attempt_id")
        attempt = await _lock_attempt(session, attempt_id)
        if attempt is None:
            raise RuntimeError(f"Attempt {attempt_id} missing for job {job.id}")
    assert_terminal_ownership(
        job,
        attempt,
        worker_id=worker_id,
        attempt_id=attempt.id,
        attempt_number=attempt_number,
        delivery_stream=delivery_stream,
        delivery_message_id=delivery_message_id,
    )
    if job.cancel_requested_at is not None:
        _finalize_owned_cancellation(job, attempt, duration_ms=duration_ms)
        return
    now = utcnow()
    attempt.status = AttemptStatus.FAILED.value
    attempt.finished_at = now
    attempt.error = error
    attempt.duration_ms = duration_ms

    retryable = bool(error.get("retryable"))
    decision = decide_retry(
        retryable=retryable,
        attempt_count=job.attempt_count,
        max_attempts=job.max_attempts,
    )
    if decision is RetryDecision.RETRY:
        settings = get_settings()
        delay = compute_retry_delay(
            attempt_number,
            base_delay_seconds=settings.retry_base_delay_seconds,
            max_delay_seconds=settings.retry_max_delay_seconds,
            jitter_ratio=settings.retry_jitter_ratio,
        )
        next_retry_at = now + timedelta(seconds=delay)
        assert_transition(JobStatus(job.status), JobStatus.RETRYING)
        job.status = JobStatus.RETRYING.value
        job.error = error
        job.result = None
        job.next_retry_at = next_retry_at
        job.worker_id = None
        job.completed_at = None
        session.add(
            create_retry_schedule_outbox_event(
                job,
                run_at=next_retry_at,
                failed_attempt=attempt_number,
            )
        )
        log_event(
            logger,
            "retry_scheduled",
            job_id=job.id,
            worker_id=worker_id,
            attempt=attempt_number,
            next_retry_at=next_retry_at.isoformat(),
            delay_seconds=delay,
        )
        return

    assert_transition(JobStatus(job.status), JobStatus.FAILED)
    job.status = JobStatus.FAILED.value
    job.result = None
    job.next_retry_at = None
    job.completed_at = now
    job.worker_id = None
    if retryable:
        last_code = error.get("code")
        job.error = {
            **_MAX_ATTEMPTS_JOB_ERROR,
            "last_error_code": last_code,
        }
        reason = DEAD_LETTER_MAX_ATTEMPTS
        log_event(
            logger,
            "max_attempts_exhausted",
            job_id=job.id,
            worker_id=worker_id,
            attempt=attempt_number,
            reason=reason,
        )
        error_code = str(last_code) if last_code is not None else None
    else:
        job.error = error
        reason = DEAD_LETTER_NON_RETRYABLE
        error_code = str(error.get("code")) if error.get("code") is not None else None
    session.add(
        create_dead_letter_outbox_event(
            job,
            reason=reason,
            error_code=error_code,
            attempt_count=job.attempt_count,
        )
    )
    log_event(
        logger,
        "dead_letter_scheduled",
        job_id=job.id,
        reason=reason,
        attempt=job.attempt_count,
        error_code=error_code,
    )


def _finalize_owned_cancellation(job: Job, attempt: JobAttempt, *, duration_ms: int) -> None:
    now = utcnow()
    attempt.status = AttemptStatus.CANCELLED.value
    attempt.finished_at = now
    attempt.error = dict(_CANCELLED_ATTEMPT_ERROR)
    attempt.duration_ms = duration_ms
    apply_terminal_cancellation(job, now=now)
    log_event(
        logger,
        "job_cancelled",
        job_id=job.id,
        worker_id=attempt.worker_id,
        attempt=attempt.attempt_number,
    )


async def persist_cooperative_cancellation(
    session: AsyncSession,
    *,
    job_id: uuid.UUID,
    attempt_id: uuid.UUID,
    attempt_number: int,
    worker_id: str,
    delivery_stream: str,
    delivery_message_id: str,
    duration_ms: int,
) -> None:
    job = await _lock_job(session, job_id)
    if job is None:
        raise RuntimeError(f"Job {job_id} disappeared during persist")
    attempt = await _lock_attempt(session, attempt_id)
    if attempt is None:
        raise RuntimeError(f"Attempt {attempt_id} missing for job {job_id}")
    assert_terminal_ownership(
        job,
        attempt,
        worker_id=worker_id,
        attempt_id=attempt_id,
        attempt_number=attempt_number,
        delivery_stream=delivery_stream,
        delivery_message_id=delivery_message_id,
    )
    _finalize_owned_cancellation(job, attempt, duration_ms=duration_ms)


async def persist_crash_cancellation(
    session: AsyncSession,
    job: Job,
    attempt: JobAttempt,
    *,
    delivery_stream: str,
    delivery_message_id: str,
) -> None:
    """Matched reclaim after cancel requested: interrupt attempt, cancel job, no new attempt."""
    _interrupt_attempt(attempt)
    log_event(
        logger,
        "attempt_interrupted",
        job_id=job.id,
        worker_id=attempt.worker_id,
        attempt=attempt.attempt_number,
        stream=delivery_stream,
        message_id=delivery_message_id,
    )
    apply_terminal_cancellation(job, now=utcnow())
    log_event(
        logger,
        "job_cancelled_after_crash",
        job_id=job.id,
        attempt=attempt.attempt_number,
        stream=delivery_stream,
        message_id=delivery_message_id,
    )


async def _safe_ack(stream: str, message_id: str) -> None:
    try:
        await ack_message(stream, message_id)
    except RedisError:
        logger.exception(
            "event=queue_error stream=%s message_id=%s XACK failed after handling",
            stream,
            message_id,
        )


def _claimed_from(job: Job, attempt: JobAttempt) -> ClaimedWork:
    return ClaimedWork(
        job_id=job.id,
        job_type=JobType(job.job_type),
        payload=dict(job.payload),
        attempt_id=attempt.id,
        attempt_number=attempt.attempt_number,
        max_attempts=job.max_attempts,
        delivery_stream=attempt.delivery_stream or "",
        delivery_message_id=attempt.delivery_message_id or "",
    )


def _delivery_matches(attempt: JobAttempt, message: StreamMessage) -> bool:
    return (
        attempt.delivery_stream == message.stream
        and attempt.delivery_message_id == message.message_id
    )


async def _reconcile_running(
    session: AsyncSession,
    job: Job,
    worker_id: str,
    message: StreamMessage,
) -> ClaimedWork | None:
    job_id = job.id
    job_status = job.status
    attempt = await _lock_active_attempt(session, job)
    if attempt is None or AttemptStatus(attempt.status) is not AttemptStatus.RUNNING:
        await session.rollback()
        log_event(
            logger,
            "unexpected_status",
            job_id=job_id,
            status=job_status,
            worker_id=worker_id,
            message_id=message.message_id,
        )
        return None
    delivery_id = attempt.delivery_message_id
    delivery_stream = attempt.delivery_stream
    attempt_number = attempt.attempt_number
    if delivery_id is None or delivery_stream is None:
        await session.rollback()
        log_event(
            logger,
            "legacy_untracked_running_attempt",
            job_id=job_id,
            attempt=attempt_number,
            stream=message.stream,
            message_id=message.message_id,
        )
        logger.warning(
            "Refusing to guess crash recovery for a RUNNING attempt without delivery identity"
        )
        return None
    if not _delivery_matches(attempt, message):
        await session.rollback()
        event_name = (
            "delivery_stream_mismatch"
            if delivery_stream != message.stream
            else "stale_duplicate_reclaimed"
        )
        log_event(
            logger,
            event_name,
            job_id=job_id,
            worker_id=worker_id,
            stream=message.stream,
            message_id=message.message_id,
            attempt=attempt_number,
        )
        await _safe_ack(message.stream, message.message_id)
        return None
    if job.cancel_requested_at is not None:
        await persist_crash_cancellation(
            session,
            job,
            attempt,
            delivery_stream=message.stream,
            delivery_message_id=message.message_id,
        )
        await session.commit()
        await _safe_ack(message.stream, message.message_id)
        return None
    if not attempt_budget_remaining(job):
        await fail_after_interrupt_budget_exhausted(
            session,
            job,
            attempt,
            delivery_stream=message.stream,
            delivery_message_id=message.message_id,
        )
        await session.commit()
        await _safe_ack(message.stream, message.message_id)
        return None
    new_attempt = await recover_crashed_attempt(
        session,
        job,
        attempt,
        worker_id,
        delivery_stream=message.stream,
        delivery_message_id=message.message_id,
    )
    claimed = _claimed_from(job, new_attempt)
    await session.commit()
    return claimed


async def process_message(
    worker_id: str,
    message: StreamMessage,
    *,
    reclaimed: bool = False,
) -> None:
    if message.job_id is None:
        log_event(
            logger,
            "poison_message",
            stream=message.stream,
            message_id=message.message_id,
        )
        logger.warning("Malformed stream message has no valid job_id; acknowledging")
        await _safe_ack(message.stream, message.message_id)
        return

    factory = get_session_factory()
    claimed: ClaimedWork | None = None
    async with factory() as session:
        job = await _lock_job(session, message.job_id)
        if job is None:
            await session.rollback()
            log_event(logger, "orphan_message", job_id=message.job_id, stream=message.stream)
            logger.warning("Redis job_id has no PostgreSQL row; acknowledging")
            await _safe_ack(message.stream, message.message_id)
            return
        status = JobStatus(job.status)
        if status in TERMINAL_STATUSES:
            await session.rollback()
            log_event(logger, "duplicate_terminal", job_id=message.job_id, status=status.value)
            await _safe_ack(message.stream, message.message_id)
            return
        if status is JobStatus.RETRYING:
            await session.rollback()
            log_event(
                logger,
                "retrying_stale_delivery",
                job_id=message.job_id,
                status=status.value,
                stream=message.stream,
                message_id=message.message_id,
            )
            await _safe_ack(message.stream, message.message_id)
            return
        if status is JobStatus.SCHEDULED:
            await session.rollback()
            log_event(
                logger,
                "scheduled_stale_delivery",
                job_id=message.job_id,
                status=status.value,
                stream=message.stream,
                message_id=message.message_id,
            )
            await _safe_ack(message.stream, message.message_id)
            return
        if status is JobStatus.QUEUED:
            if not attempt_budget_remaining(job):
                await fail_queued_budget_exhausted(session, job)
                await session.commit()
                await _safe_ack(message.stream, message.message_id)
                return
            attempt = await claim_queued_job(
                session,
                job,
                worker_id,
                delivery_stream=message.stream,
                delivery_message_id=message.message_id,
            )
            claimed = _claimed_from(job, attempt)
            await session.commit()
        elif status is JobStatus.RUNNING:
            if reclaimed:
                claimed = await _reconcile_running(session, job, worker_id, message)
            else:
                await session.rollback()
                log_event(
                    logger,
                    "unexpected_status",
                    job_id=message.job_id,
                    status=status.value,
                )
                logger.warning("Leaving non-QUEUED in-flight delivery pending for reclaim")
                return
        else:
            await session.rollback()
            log_event(
                logger,
                "unexpected_status",
                job_id=message.job_id,
                status=status.value,
            )
            logger.warning("Conservative skip of non-operational status; not executing")
            return

    if claimed is None:
        return

    log_event(
        logger,
        "job_claimed",
        job_id=claimed.job_id,
        worker_id=worker_id,
        attempt=claimed.attempt_number,
        stream=claimed.delivery_stream,
        message_id=claimed.delivery_message_id,
    )
    log_event(
        logger,
        "task_started",
        job_id=claimed.job_id,
        worker_id=worker_id,
        attempt=claimed.attempt_number,
        stream=claimed.delivery_stream,
        message_id=claimed.delivery_message_id,
    )

    started = time.perf_counter()
    success = False
    cancelled = False
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    context = TaskExecutionContext(claimed.job_id)
    try:
        await context.checkpoint()
        result = await execute_handler(claimed.job_type, claimed.payload, context)
        success = True
        log_event(logger, "task_succeeded", job_id=claimed.job_id, worker_id=worker_id)
    except TaskCancelledError:
        cancelled = True
        log_event(
            logger,
            "task_cancelled",
            job_id=claimed.job_id,
            worker_id=worker_id,
            attempt=claimed.attempt_number,
        )
    except TaskExecutionError as exc:
        error = classify_handler_exception(exc)
        event_name = "task_retryable_failure" if exc.retryable else "task_non_retryable_failure"
        log_event(
            logger,
            event_name,
            job_id=claimed.job_id,
            worker_id=worker_id,
            attempt=claimed.attempt_number,
            error=error["code"],
        )
    except Exception:
        logger.exception("event=task_failed job_id=%s", claimed.job_id)
        error = {
            "code": "TASK_EXECUTION_FAILED",
            "message": "Task execution failed.",
            "retryable": False,
        }
        log_event(
            logger,
            "task_non_retryable_failure",
            job_id=claimed.job_id,
            worker_id=worker_id,
            attempt=claimed.attempt_number,
            error=error["code"],
        )
    duration_ms = max(0, int((time.perf_counter() - started) * 1000))

    async with factory() as session:
        try:
            if cancelled:
                await persist_cooperative_cancellation(
                    session,
                    job_id=claimed.job_id,
                    attempt_id=claimed.attempt_id,
                    attempt_number=claimed.attempt_number,
                    worker_id=worker_id,
                    delivery_stream=claimed.delivery_stream,
                    delivery_message_id=claimed.delivery_message_id,
                    duration_ms=duration_ms,
                )
            elif success:
                await persist_terminal_outcome(
                    session,
                    job_id=claimed.job_id,
                    attempt_id=claimed.attempt_id,
                    attempt_number=claimed.attempt_number,
                    worker_id=worker_id,
                    delivery_stream=claimed.delivery_stream,
                    delivery_message_id=claimed.delivery_message_id,
                    success=True,
                    result=result,
                    error=None,
                    duration_ms=duration_ms,
                )
            else:
                await persist_handler_failure(
                    session,
                    job_id=claimed.job_id,
                    attempt_id=claimed.attempt_id,
                    attempt_number=claimed.attempt_number,
                    worker_id=worker_id,
                    delivery_stream=claimed.delivery_stream,
                    delivery_message_id=claimed.delivery_message_id,
                    error=error
                    or {
                        "code": "TASK_EXECUTION_FAILED",
                        "message": "Task execution failed.",
                        "retryable": False,
                    },
                    duration_ms=duration_ms,
                )
            await session.commit()
        except WorkerOwnershipError:
            log_event(
                logger,
                "ownership_conflict",
                job_id=claimed.job_id,
                worker_id=worker_id,
                attempt=claimed.attempt_number,
                stream=claimed.delivery_stream,
                message_id=claimed.delivery_message_id,
            )
            logger.exception("Refusing stale terminal persist; leaving Redis pending")
            await session.rollback()
            return
        except SQLAlchemyError:
            logger.exception("event=database_error job_id=%s", claimed.job_id)
            await session.rollback()
            return

    await _safe_ack(message.stream, message.message_id)
