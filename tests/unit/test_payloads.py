import pytest
from pydantic import ValidationError

from job_platform.core.enums import JobType
from job_platform.core.errors import AppError
from job_platform.tasks.definitions import (
    assert_payload_size,
    parse_job_type,
    validate_payload,
)


def test_parse_known_job_types() -> None:
    for value in JobType:
        assert parse_job_type(value.value) is value


def test_unknown_job_type() -> None:
    with pytest.raises(AppError) as exc_info:
        parse_job_type("delete_everything")
    assert exc_info.value.code == "UNKNOWN_JOB_TYPE"
    assert exc_info.value.status_code == 422


def test_word_count_payload() -> None:
    assert validate_payload(JobType.WORD_COUNT, {"text": "hello world"}) == {"text": "hello world"}
    assert validate_payload(JobType.WORD_COUNT, {"text": ""}) == {"text": ""}


def test_word_count_rejects_wrong_type() -> None:
    with pytest.raises(AppError) as exc_info:
        validate_payload(JobType.WORD_COUNT, {"text": 123})
    assert exc_info.value.code == "INVALID_PAYLOAD"


def test_word_count_rejects_unknown_fields() -> None:
    with pytest.raises(AppError):
        validate_payload(JobType.WORD_COUNT, {"text": "hi", "extra": True})


def test_sum_numbers_payload() -> None:
    assert validate_payload(JobType.SUM_NUMBERS, {"numbers": [1, 2, 3.5]}) == {
        "numbers": [1.0, 2.0, 3.5]
    }


def test_sum_numbers_rejects_non_finite() -> None:
    with pytest.raises(AppError):
        validate_payload(JobType.SUM_NUMBERS, {"numbers": [1, float("inf")]})


def test_sleep_payload() -> None:
    assert validate_payload(JobType.SLEEP, {"seconds": 1.5}) == {"seconds": 1.5}
    assert validate_payload(JobType.SLEEP, {"seconds": 0}) == {"seconds": 0.0}


def test_sleep_rejects_negative() -> None:
    with pytest.raises(AppError):
        validate_payload(JobType.SLEEP, {"seconds": -1})


def test_sleep_rejects_above_thirty() -> None:
    with pytest.raises(AppError) as exc_info:
        validate_payload(JobType.SLEEP, {"seconds": 31})
    assert exc_info.value.code == "INVALID_PAYLOAD"


def test_sleep_accepts_thirty() -> None:
    assert validate_payload(JobType.SLEEP, {"seconds": 30}) == {"seconds": 30.0}


def test_prime_calculation_payload() -> None:
    assert validate_payload(JobType.PRIME_CALCULATION, {"limit": 10000}) == {"limit": 10000}


def test_prime_calculation_rejects_below_two() -> None:
    with pytest.raises(AppError):
        validate_payload(JobType.PRIME_CALCULATION, {"limit": 1})


def test_prime_calculation_rejects_above_limit() -> None:
    with pytest.raises(AppError) as exc_info:
        validate_payload(JobType.PRIME_CALCULATION, {"limit": 100_001})
    assert exc_info.value.code == "INVALID_PAYLOAD"


def test_prime_calculation_accepts_max() -> None:
    assert validate_payload(JobType.PRIME_CALCULATION, {"limit": 100_000}) == {"limit": 100000}


def test_simulate_failure_empty_object() -> None:
    assert validate_payload(JobType.SIMULATE_FAILURE, {}) == {"mode": "non_retryable"}


def test_simulate_failure_retryable_mode() -> None:
    assert validate_payload(JobType.SIMULATE_FAILURE, {"mode": "retryable"}) == {
        "mode": "retryable"
    }


def test_simulate_failure_invalid_mode() -> None:
    with pytest.raises(AppError):
        validate_payload(JobType.SIMULATE_FAILURE, {"mode": "explode"})


def test_simulate_failure_rejects_fields() -> None:
    with pytest.raises(AppError):
        validate_payload(JobType.SIMULATE_FAILURE, {"reason": "nope"})


def test_payload_size_limit() -> None:
    payload = {"text": "x"}
    assert_payload_size(payload, max_bytes=32)
    with pytest.raises(AppError) as exc_info:
        assert_payload_size({"text": "x" * 100}, max_bytes=16)
    assert exc_info.value.code == "PAYLOAD_TOO_LARGE"


def test_programming_bugs_are_not_invalid_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    from unittest.mock import Mock

    from job_platform.tasks.definitions import WordCountPayload

    monkeypatch.setattr(
        WordCountPayload,
        "model_validate",
        Mock(side_effect=RuntimeError("unexpected bug")),
    )
    with pytest.raises(RuntimeError, match="unexpected bug"):
        validate_payload(JobType.WORD_COUNT, {"text": "x"})


def test_word_count_schema_direct_unknown_field() -> None:
    from job_platform.tasks.definitions import WordCountPayload

    with pytest.raises(ValidationError):
        WordCountPayload.model_validate({"text": "a", "other": 1})
