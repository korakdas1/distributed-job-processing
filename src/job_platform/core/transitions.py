"""Allowed job status transitions.

Normal work: QUEUED -> RUNNING -> SUCCEEDED | RETRYING | FAILED.
RUNNING -> QUEUED is crash-recovery ownership transfer, not task retry.
RETRYING -> QUEUED is retry-scheduler promotion after next_retry_at.
QUEUED -> FAILED is defensive attempt-budget exhaustion.
"""

from job_platform.core.enums import JobStatus

_ALLOWED: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.SCHEDULED: frozenset({JobStatus.QUEUED, JobStatus.CANCELLED}),
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.FAILED, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset(
        {
            JobStatus.SUCCEEDED,
            JobStatus.RETRYING,
            JobStatus.FAILED,
            JobStatus.QUEUED,
            JobStatus.CANCELLED,
        }
    ),
    JobStatus.RETRYING: frozenset({JobStatus.QUEUED, JobStatus.FAILED, JobStatus.CANCELLED}),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.CANCELLED: frozenset(),
}

TERMINAL_STATUSES: frozenset[JobStatus] = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}
)


def allowed_transitions(status: JobStatus) -> frozenset[JobStatus]:
    return _ALLOWED[status]


def can_transition(current: JobStatus, target: JobStatus) -> bool:
    return target in _ALLOWED[current]


def assert_transition(current: JobStatus, target: JobStatus) -> None:
    if not can_transition(current, target):
        msg = f"Illegal job transition {current} -> {target}"
        raise ValueError(msg)
