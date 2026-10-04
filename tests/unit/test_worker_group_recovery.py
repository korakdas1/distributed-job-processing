"""Runtime error classification and bounded consumer-group repair retries."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import ConnectionError, RedisError, ResponseError, TimeoutError

from job_platform.worker import processor, runtime


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ResponseError("NOGROUP No such key 'jobs:normal' or consumer group 'job-workers'"), True),
        (ResponseError("NOGROUP No such key or consumer group"), True),
        (ResponseError("NOGROUP"), True),
        (ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value"), False),
        (ResponseError("ERR mentions NOGROUP but is not that error code"), False),
        (ResponseError("NOGROUPISH invalid"), False),
        (ResponseError("BUSYGROUP Consumer Group name already exists"), False),
        (ResponseError(""), False),
        (ConnectionError("NOGROUP"), False),
        (TimeoutError("NOGROUP"), False),
    ],
)
def test_nogroup_classification(error: RedisError, expected: bool) -> None:
    assert runtime.is_nogroup_error(error) is expected


@pytest.mark.parametrize("source", ["read", "reclaim"])
async def test_nogroup_repairs_then_returns_to_normal_loop(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, source: str
) -> None:
    stop = asyncio.Event()
    ensure = AsyncMock()
    error = ResponseError("NOGROUP No such key or consumer group")
    reclaim = AsyncMock(side_effect=[error, (None, 0)] if source == "reclaim" else [(None, 0)])
    reads = 0

    async def read(*_args: object) -> None:
        nonlocal reads
        reads += 1
        if source == "read" and reads == 1:
            raise error
        stop.set()

    monkeypatch.setattr(runtime, "ensure_consumer_groups", ensure)
    monkeypatch.setattr(runtime, "_reclaim_one", reclaim)
    monkeypatch.setattr(runtime, "_read_fair", read)
    with caplog.at_level(logging.INFO):
        await asyncio.wait_for(runtime.worker_loop("worker-test", stop=stop), timeout=2)
    ensure.assert_awaited_once()
    assert "event=consumer_group_recovery_started" in caplog.text
    assert "event=consumer_group_recovery_succeeded" in caplog.text
    assert "event=consumer_group_recovery_failed" not in caplog.text


@pytest.mark.parametrize(
    "error",
    [ResponseError("WRONGTYPE invalid key type"), ConnectionError("offline"), TimeoutError()],
)
async def test_other_redis_errors_only_pause(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, error: RedisError
) -> None:
    stop = asyncio.Event()
    pauses: list[float] = []

    async def pause(delay: float) -> None:
        pauses.append(delay)
        stop.set()

    ensure = AsyncMock()
    monkeypatch.setattr(runtime, "ensure_consumer_groups", ensure)
    monkeypatch.setattr(runtime, "_reclaim_one", AsyncMock(side_effect=error))
    monkeypatch.setattr(runtime.asyncio, "sleep", pause)
    with caplog.at_level(logging.INFO):
        await runtime.worker_loop("worker-test", stop=stop)
    ensure.assert_not_awaited()
    assert pauses == [runtime._INFRA_PAUSE_SECONDS]
    assert "event=queue_error" in caplog.text
    assert "consumer_group_recovery" not in caplog.text


@pytest.mark.parametrize("stop_during_pause", [False, True])
async def test_failed_repair_pauses_and_retries_or_stops(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    stop_during_pause: bool,
) -> None:
    stop = asyncio.Event()
    pauses: list[float] = []
    ensure = AsyncMock(side_effect=[ConnectionError("offline during repair"), None])

    async def pause(delay: float) -> None:
        pauses.append(delay)
        if stop_during_pause:
            stop.set()

    async def read(*_args: object) -> None:
        if ensure.await_count < 2:
            raise ResponseError("NOGROUP No such key or consumer group")
        stop.set()

    monkeypatch.setattr(runtime, "ensure_consumer_groups", ensure)
    monkeypatch.setattr(runtime, "_reclaim_one", AsyncMock(return_value=(None, 0)))
    monkeypatch.setattr(runtime, "_read_fair", read)
    monkeypatch.setattr(runtime.asyncio, "sleep", pause)
    with caplog.at_level(logging.INFO):
        await runtime.worker_loop("worker-test", stop=stop)
    assert pauses == [runtime._INFRA_PAUSE_SECONDS]
    assert ensure.await_count == (1 if stop_during_pause else 2)
    assert "event=consumer_group_recovery_failed" in caplog.text
    assert ("event=consumer_group_recovery_succeeded" in caplog.text) is not stop_during_pause


async def test_ack_nogroup_remains_best_effort(monkeypatch: pytest.MonkeyPatch) -> None:
    ack = AsyncMock(side_effect=ResponseError("NOGROUP No such consumer group"))
    monkeypatch.setattr(processor, "ack_message", ack)
    await processor._safe_ack("jobs:normal", "1-0")
    ack.assert_awaited_once_with("jobs:normal", "1-0")
