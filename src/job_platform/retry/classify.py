"""Classify handler exceptions into safe public job/attempt error dictionaries."""

from __future__ import annotations

from typing import Any

from job_platform.tasks.errors import TaskExecutionError

TASK_EXECUTION_FAILED = "TASK_EXECUTION_FAILED"


def classify_handler_exception(exc: Exception) -> dict[str, Any]:
    """Return a small client-safe error dict. Tracebacks stay in application logs."""
    if isinstance(exc, TaskExecutionError):
        return exc.to_error_dict()
    return {
        "code": TASK_EXECUTION_FAILED,
        "message": "Task execution failed.",
        "retryable": False,
    }
