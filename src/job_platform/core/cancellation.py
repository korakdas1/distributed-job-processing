"""Apply terminal CANCELLED fields. PostgreSQL remains authoritative."""

from __future__ import annotations

from datetime import datetime

from job_platform.core.enums import JobStatus
from job_platform.core.transitions import assert_transition
from job_platform.models.job import Job


def apply_terminal_cancellation(job: Job, *, now: datetime) -> None:
    """Move a locked job to terminal CANCELLED. Preserves cancel_requested_at."""
    assert_transition(JobStatus(job.status), JobStatus.CANCELLED)
    if job.cancel_requested_at is None:
        job.cancel_requested_at = now
    job.cancelled_at = now
    job.completed_at = now
    job.status = JobStatus.CANCELLED.value
    job.worker_id = None
    job.next_retry_at = None
    job.result = None
    job.error = None
