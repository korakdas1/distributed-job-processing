from job_platform.retry.classify import classify_handler_exception
from job_platform.tasks.errors import NonRetryableTaskError, RetryableTaskError


def test_retryable_task_error_metadata() -> None:
    exc = RetryableTaskError("SIMULATED_RETRYABLE_FAILURE", "temporary")
    assert exc.retryable is True
    assert exc.to_error_dict() == {
        "code": "SIMULATED_RETRYABLE_FAILURE",
        "message": "temporary",
        "retryable": True,
    }


def test_non_retryable_task_error_metadata() -> None:
    exc = NonRetryableTaskError("SIMULATED_FAILURE", "permanent")
    assert exc.retryable is False
    assert exc.to_error_dict()["retryable"] is False


def test_unexpected_exception_is_safe_non_retryable() -> None:
    public = classify_handler_exception(RuntimeError("secret traceback stuff"))
    assert public == {
        "code": "TASK_EXECUTION_FAILED",
        "message": "Task execution failed.",
        "retryable": False,
    }
    assert "traceback" not in public
    assert "secret" not in public["message"]
