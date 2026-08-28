from job_platform.core.enums import JobPriority, JobStatus
from job_platform.core.transitions import (
    TERMINAL_STATUSES,
    allowed_transitions,
    assert_transition,
    can_transition,
)


def test_all_statuses_have_transition_entries() -> None:
    for status in JobStatus:
        allowed_transitions(status)


def test_allowed_transition_table() -> None:
    allowed = {
        JobStatus.SCHEDULED: {JobStatus.QUEUED, JobStatus.CANCELLED},
        JobStatus.QUEUED: {JobStatus.RUNNING, JobStatus.FAILED, JobStatus.CANCELLED},
        JobStatus.RUNNING: {
            JobStatus.SUCCEEDED,
            JobStatus.RETRYING,
            JobStatus.FAILED,
            JobStatus.QUEUED,
            JobStatus.CANCELLED,
        },
        JobStatus.RETRYING: {JobStatus.QUEUED, JobStatus.FAILED, JobStatus.CANCELLED},
        JobStatus.SUCCEEDED: set(),
        JobStatus.FAILED: set(),
        JobStatus.CANCELLED: set(),
    }
    for current, targets in allowed.items():
        assert allowed_transitions(current) == frozenset(targets)
        for target in JobStatus:
            assert can_transition(current, target) is (target in targets)


def test_illegal_transition_raises() -> None:
    try:
        assert_transition(JobStatus.SUCCEEDED, JobStatus.QUEUED)
    except ValueError as exc:
        assert "Illegal" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_terminal_statuses() -> None:
    assert {
        JobStatus.SUCCEEDED,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    } == TERMINAL_STATUSES


def test_priority_values() -> None:
    assert JobPriority.NORMAL.value == "NORMAL"
    assert JobPriority("HIGH") is JobPriority.HIGH
