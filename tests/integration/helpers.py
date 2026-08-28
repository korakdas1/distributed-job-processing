"""Shared helpers for Redis stream + worker integration tests."""

from __future__ import annotations

import time

from job_platform.core.config import get_settings
from job_platform.outbox.publisher import publish_available
from job_platform.queue.client import get_redis
from job_platform.queue.priority import ready_streams
from job_platform.queue.streams import (
    StreamMessage,
    ensure_consumer_groups,
    pending_count,
    read_one_from_stream,
)
from job_platform.worker.processor import process_message

TEST_WORKER_ID = "worker-test0001"


async def stream_entries(stream: str | None = None) -> list[tuple[str, dict[str, str]]]:
    settings = get_settings()
    name = settings.redis_stream_normal if stream is None else stream
    raw = await get_redis().xrange(name)
    entries: list[tuple[str, dict[str, str]]] = []
    for message_id, fields in raw:
        mapping = {str(key): str(value) for key, value in fields.items()}
        entries.append((str(message_id), mapping))
    return entries


async def drain_outbox(*, limit: int = 100) -> int:
    return await publish_available(limit=limit)


async def process_next_message(
    worker_id: str = TEST_WORKER_ID,
    *,
    reclaimed: bool = False,
) -> StreamMessage | None:
    await ensure_consumer_groups()
    await drain_outbox()
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        for stream in ready_streams():
            message = await read_one_from_stream(stream, worker_id, block_ms=50)
            if message is not None:
                await process_message(worker_id, message, reclaimed=reclaimed)
                return message
    return None


async def consumer_pending(stream: str | None = None) -> int:
    await ensure_consumer_groups()
    return await pending_count(stream)


async def delayed_members() -> list[tuple[str, float]]:
    settings = get_settings()
    raw = await get_redis().zrange(settings.redis_delayed_zset, 0, -1, withscores=True)
    return [(str(member), float(score)) for member, score in raw]


async def dead_letter_entries() -> list[tuple[str, dict[str, str]]]:
    settings = get_settings()
    raw = await get_redis().xrange(settings.redis_dead_letter_stream)
    entries: list[tuple[str, dict[str, str]]] = []
    for message_id, fields in raw:
        mapping = {str(key): str(value) for key, value in fields.items()}
        entries.append((str(message_id), mapping))
    return entries
