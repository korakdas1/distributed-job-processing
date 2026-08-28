"""Explicit task failure types. Unexpected Python exceptions are classified separately."""

from __future__ import annotations

from typing import Any


class TaskExecutionError(Exception):
    """Handler-visible task failure with safe public metadata."""

    def __init__(self, code: str, message: str, *, retryable: bool) -> None:
        self.code = code
        self.message = message
        self.retryable = retryable
        super().__init__(message)

    def to_error_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
        }


class RetryableTaskError(TaskExecutionError):
    """The handler ran and failed, but another attempt may help if budget remains."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message, retryable=True)


class NonRetryableTaskError(TaskExecutionError):
    """Retrying is not expected to help. Goes directly to FAILED + dead-letter."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message, retryable=False)


class TaskCancelledError(Exception):
    """Cooperative cancellation. Not a retryable or non-retryable task failure."""
