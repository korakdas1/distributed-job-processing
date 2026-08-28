import pytest

from job_platform.core.enums import JobType
from job_platform.tasks.errors import RetryableTaskError
from job_platform.tasks.handlers import (
    HANDLERS,
    SimulatedTaskFailure,
    UnknownHandlerError,
    execute_handler,
    handle_prime_calculation,
    handle_simulate_failure,
    handle_sleep,
    handle_sum_numbers,
    handle_word_count,
)


async def test_word_count_hello_world() -> None:
    assert await handle_word_count({"text": "hello world"}) == {"word_count": 2}


async def test_word_count_empty() -> None:
    assert await handle_word_count({"text": ""}) == {"word_count": 0}


async def test_word_count_whitespace() -> None:
    assert await handle_word_count({"text": "  hello   distributed\tworld\n"}) == {"word_count": 3}


async def test_sum_numbers() -> None:
    assert await handle_sum_numbers({"numbers": [1, 2, 3.5]}) == {"sum": 6.5}


async def test_sum_numbers_empty() -> None:
    assert await handle_sum_numbers({"numbers": []}) == {"sum": 0}


async def test_sleep_short() -> None:
    result = await handle_sleep({"seconds": 0.01})
    assert result == {"slept_seconds": 0.01}


async def test_prime_limit_2() -> None:
    assert await handle_prime_calculation({"limit": 2}) == {
        "count": 1,
        "largest_prime": 2,
    }


async def test_prime_limit_10() -> None:
    assert await handle_prime_calculation({"limit": 10}) == {
        "count": 4,
        "largest_prime": 7,
    }


async def test_prime_limit_100() -> None:
    assert await handle_prime_calculation({"limit": 100}) == {
        "count": 25,
        "largest_prime": 97,
    }


async def test_simulate_failure_raises() -> None:
    with pytest.raises(SimulatedTaskFailure):
        await handle_simulate_failure({})


async def test_simulate_failure_retryable_mode_raises() -> None:
    with pytest.raises(RetryableTaskError) as exc_info:
        await handle_simulate_failure({"mode": "retryable"})
    assert exc_info.value.code == "SIMULATED_RETRYABLE_FAILURE"
    assert exc_info.value.retryable is True


async def test_every_job_type_has_handler() -> None:
    assert set(HANDLERS) == set(JobType)


async def test_registry_lookup_is_deterministic() -> None:
    assert HANDLERS[JobType.WORD_COUNT] is handle_word_count
    first = await execute_handler(JobType.WORD_COUNT, {"text": "a b"})
    second = await execute_handler(JobType.WORD_COUNT, {"text": "a b"})
    assert first == second == {"word_count": 2}


async def test_unregistered_type_cannot_execute(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("job_platform.tasks.handlers.HANDLERS", {})
    with pytest.raises(UnknownHandlerError):
        await execute_handler(JobType.WORD_COUNT, {"text": "x"})
