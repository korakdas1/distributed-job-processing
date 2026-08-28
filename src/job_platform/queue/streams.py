"""Redis Streams dispatch.

PostgreSQL remains the source of truth. Stream entries carry job_id (and
optional outbox_event_id). Redis distributes deliveries; PostgreSQL row locks
protect logical claiming. Publication is at-least-once via the outbox.

Ready work uses four priority streams. Each stream has its own job-workers
consumer group starting at ID 0. jobs:dead is not a ready stream.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

from redis.exceptions import ResponseError

from job_platform.core.config import get_settings
from job_platform.core.logging import log_event
from job_platform.queue.client import get_redis
from job_platform.queue.priority import ready_streams

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StreamMessage:
    stream: str
    message_id: str
    job_id: uuid.UUID | None
    fields: dict[str, str]


def parse_job_id(fields: Mapping[str, str]) -> uuid.UUID | None:
    raw = fields.get("job_id")
    if raw is None or raw == "":
        return None
    try:
        return uuid.UUID(str(raw))
    except ValueError:
        return None


def _fields_mapping(fields: object) -> dict[str, str]:
    if not isinstance(fields, dict):
        return {}
    return {str(key): str(value) for key, value in fields.items()}


def _message_from(stream: str, message_id: object, fields: object) -> StreamMessage:
    mapping = _fields_mapping(fields)
    return StreamMessage(
        stream=stream,
        message_id=str(message_id),
        job_id=parse_job_id(mapping),
        fields=mapping,
    )


async def publish_job_id(
    job_id: uuid.UUID,
    *,
    stream: str,
    outbox_event_id: uuid.UUID | None = None,
) -> str:
    """XADD a dispatch pointer onto the given ready stream. Payload stays in PostgreSQL."""
    payload: dict[str, str] = {"job_id": str(job_id)}
    if outbox_event_id is not None:
        payload["outbox_event_id"] = str(outbox_event_id)
    message_id = await get_redis().xadd(stream, cast(Any, payload))
    log_event(
        logger,
        "job_published",
        job_id=job_id,
        stream=stream,
        message_id=message_id,
        event_id=outbox_event_id,
    )
    return str(message_id)


async def ensure_consumer_group(stream_name: str) -> None:
    """Create the group from ID 0 so messages published before the worker are readable."""
    settings = get_settings()
    try:
        await get_redis().xgroup_create(
            name=stream_name,
            groupname=settings.redis_consumer_group,
            id="0",
            mkstream=True,
        )
        log_event(
            logger,
            "consumer_group_created",
            stream=stream_name,
            group=settings.redis_consumer_group,
        )
    except ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            return
        raise


async def ensure_consumer_groups() -> None:
    for stream_name in ready_streams():
        await ensure_consumer_group(stream_name)


async def read_one_from_stream(
    stream: str,
    consumer_name: str,
    *,
    block_ms: int,
    count: int = 1,
) -> StreamMessage | None:
    settings = get_settings()
    # Redis BLOCK 0 means wait forever. Non-positive values must omit BLOCK
    # so empty high-priority streams do not stall lower-priority work.
    block = None if block_ms <= 0 else block_ms
    result = await get_redis().xreadgroup(
        groupname=settings.redis_consumer_group,
        consumername=consumer_name,
        streams={stream: ">"},
        count=count,
        block=block,
    )
    if not result:
        return None
    stream_name, messages = result[0]
    if not messages:
        return None
    message_id, fields = messages[0]
    return _message_from(str(stream_name), message_id, fields)


async def read_one(consumer_name: str, *, block_ms: int) -> StreamMessage | None:
    """Compatibility wrapper: read one new message from the NORMAL ready stream."""
    settings = get_settings()
    return await read_one_from_stream(
        settings.redis_stream_normal,
        consumer_name,
        block_ms=block_ms,
    )


async def autoclaim_stale(
    stream: str,
    consumer_name: str,
    *,
    min_idle_ms: int,
    start_id: str = "0-0",
    count: int = 1,
) -> StreamMessage | None:
    """Claim one pending entry idle longer than the processing lease on one stream."""
    settings = get_settings()
    result = await get_redis().xautoclaim(
        name=stream,
        groupname=settings.redis_consumer_group,
        consumername=consumer_name,
        min_idle_time=min_idle_ms,
        start_id=start_id,
        count=count,
    )
    if not result:
        return None
    messages = result[1] if len(result) > 1 else []
    if not messages:
        return None
    message_id, fields = messages[0]
    claimed = _message_from(stream, message_id, fields)
    log_event(
        logger,
        "reclaim_received",
        worker_id=consumer_name,
        job_id=claimed.job_id,
        stream=claimed.stream,
        message_id=claimed.message_id,
    )
    return claimed


async def ack_message(stream: str, message_id: str) -> None:
    settings = get_settings()
    await get_redis().xack(stream, settings.redis_consumer_group, message_id)
    log_event(logger, "message_acked", stream=stream, message_id=message_id)


async def pending_count(stream: str | None = None) -> int:
    settings = get_settings()
    streams = (stream,) if stream is not None else ready_streams()
    total = 0
    for name in streams:
        info = await get_redis().xpending(name, settings.redis_consumer_group)
        if info is None:
            continue
        if isinstance(info, dict):
            total += int(info.get("pending", 0))
        else:
            total += int(info[0])
    return total


async def list_consumer_names(stream: str | None = None) -> list[str]:
    """Consumer names currently known to the job-workers group on one ready stream."""
    settings = get_settings()
    target = settings.redis_stream_normal if stream is None else stream
    info = await get_redis().xinfo_consumers(target, settings.redis_consumer_group)
    names: list[str] = []
    for item in info:
        name = str(item.get("name") or "") if isinstance(item, dict) else str(item[0])
        if name:
            names.append(name)
    return names
