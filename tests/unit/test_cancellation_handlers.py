import uuid

import pytest

from job_platform.tasks.context import NullTaskContext, TaskExecutionContext
from job_platform.tasks.errors import TaskCancelledError
from job_platform.tasks.handlers import handle_prime_calculation, handle_sleep, handle_word_count


class _CancelAfter:
    def __init__(self, after: int) -> None:
        self.after = after
        self.n = 0

    @property
    def sleep_chunk_seconds(self) -> float:
        return 0.05

    async def checkpoint(self) -> None:
        self.n += 1
        if self.n >= self.after:
            raise TaskCancelledError()


async def test_sleep_checkpoints_during_wait() -> None:
    ctx = _CancelAfter(after=2)
    with pytest.raises(TaskCancelledError):
        await handle_sleep({"seconds": 5}, ctx)  # type: ignore[arg-type]
    assert ctx.n >= 2


async def test_prime_calculation_checkpoints() -> None:
    ctx = _CancelAfter(after=2)
    with pytest.raises(TaskCancelledError):
        await handle_prime_calculation({"limit": 10_000}, ctx)  # type: ignore[arg-type]
    assert ctx.n >= 2


async def test_word_count_still_works_with_null_context() -> None:
    assert await handle_word_count({"text": "a b"}, NullTaskContext()) == {"word_count": 2}


async def test_null_context_never_cancels_sleep() -> None:
    result = await handle_sleep({"seconds": 0.01}, NullTaskContext())
    assert result == {"slept_seconds": 0.01}


def test_task_execution_context_interval_bounds() -> None:
    ctx = TaskExecutionContext(uuid.uuid4(), poll_interval_ms=50)
    assert ctx.sleep_chunk_seconds == 0.05
