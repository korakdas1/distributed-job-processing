"""Lazy Redis asyncio client. Importing the API does not require Redis to be up."""

from __future__ import annotations

import asyncio
from contextlib import suppress

from redis.asyncio import Redis

from job_platform.core.config import get_settings

_client: Redis | None = None
_loop: asyncio.AbstractEventLoop | None = None


def get_redis() -> Redis:
    """Return a client bound to the current event loop."""
    global _client, _loop
    loop = asyncio.get_running_loop()
    if _client is None or _loop is not loop:
        settings = get_settings()
        _client = Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=5,
        )
        _loop = loop
    return _client


async def dispose_redis() -> None:
    global _client, _loop
    client = _client
    _client = None
    _loop = None
    if client is None:
        return
    with suppress(RuntimeError):
        await client.aclose()


async def ping_redis() -> None:
    await get_redis().ping()
