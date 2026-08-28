"""Transactional outbox event constructors. Payload is event metadata only."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from job_platform.core.clock import utcnow
from job_platform.models.job import Job
from job_platform.models.outbox import (
    OUTBOX_EVENT_JOB_DEAD_LETTER,
    OUTBOX_EVENT_JOB_DISPATCH,
    OUTBOX_EVENT_JOB_INITIAL_SCHEDULE,
    OUTBOX_EVENT_JOB_RETRY_SCHEDULE,
    OutboxEvent,
)

DEAD_LETTER_NON_RETRYABLE = "NON_RETRYABLE_FAILURE"
DEAD_LETTER_MAX_ATTEMPTS = "MAX_ATTEMPTS_EXHAUSTED"
DEAD_LETTER_BUDGET_AFTER_INTERRUPT = "ATTEMPT_BUDGET_EXHAUSTED_AFTER_INTERRUPT"


def create_dispatch_outbox_event(job: Job) -> OutboxEvent:
    return OutboxEvent(
        id=uuid.uuid4(),
        job_id=job.id,
        event_type=OUTBOX_EVENT_JOB_DISPATCH,
        payload={"priority": job.priority},
        created_at=utcnow(),
        published_at=None,
        publish_attempts=0,
        last_error=None,
        redis_message_id=None,
    )


def create_initial_schedule_outbox_event(job: Job, *, run_at: datetime) -> OutboxEvent:
    aware = run_at if run_at.tzinfo is not None else run_at.replace(tzinfo=UTC)
    return OutboxEvent(
        id=uuid.uuid4(),
        job_id=job.id,
        event_type=OUTBOX_EVENT_JOB_INITIAL_SCHEDULE,
        payload={
            "run_at": aware.astimezone(UTC).isoformat(),
            "priority": job.priority,
        },
        created_at=utcnow(),
        published_at=None,
        publish_attempts=0,
        last_error=None,
        redis_message_id=None,
    )


def create_retry_schedule_outbox_event(
    job: Job,
    *,
    run_at: datetime,
    failed_attempt: int,
) -> OutboxEvent:
    aware = run_at if run_at.tzinfo is not None else run_at.replace(tzinfo=UTC)
    return OutboxEvent(
        id=uuid.uuid4(),
        job_id=job.id,
        event_type=OUTBOX_EVENT_JOB_RETRY_SCHEDULE,
        payload={
            "run_at": aware.astimezone(UTC).isoformat(),
            "failed_attempt": failed_attempt,
        },
        created_at=utcnow(),
        published_at=None,
        publish_attempts=0,
        last_error=None,
        redis_message_id=None,
    )


def create_dead_letter_outbox_event(
    job: Job,
    *,
    reason: str,
    error_code: str | None,
    attempt_count: int,
) -> OutboxEvent:
    payload: dict[str, Any] = {
        "reason": reason,
        "attempt_count": attempt_count,
        "priority": job.priority,
    }
    if error_code is not None:
        payload["error_code"] = error_code
    return OutboxEvent(
        id=uuid.uuid4(),
        job_id=job.id,
        event_type=OUTBOX_EVENT_JOB_DEAD_LETTER,
        payload=payload,
        created_at=utcnow(),
        published_at=None,
        publish_attempts=0,
        last_error=None,
        redis_message_id=None,
    )
