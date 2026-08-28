"""Independent outbox publisher process. Not started by FastAPI.

XADD only. Workers own consumer-group creation. Redis may be down at startup:
the process stays alive, logs, pauses, and retries until Redis returns.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from job_platform.core.config import get_settings
from job_platform.core.logging import configure_logging, log_event
from job_platform.db.session import dispose_engine
from job_platform.outbox.publisher import publish_available
from job_platform.queue.client import dispose_redis

logger = logging.getLogger(__name__)

_INFRA_PAUSE_SECONDS = 1.0


async def publisher_loop(*, stop: asyncio.Event | None = None) -> None:
    log_event(logger, "publisher_ready", pid=os.getpid())
    settings = get_settings()
    poll_seconds = settings.outbox_poll_interval_ms / 1000.0
    while stop is None or not stop.is_set():
        try:
            published = await publish_available()
            if published == 0:
                await asyncio.sleep(poll_seconds)
        except RedisError:
            log_event(logger, "outbox_publish_failed")
            logger.exception("Redis publish failed; retrying after a short pause")
            await asyncio.sleep(_INFRA_PAUSE_SECONDS)
        except SQLAlchemyError:
            log_event(logger, "database_error")
            logger.exception("PostgreSQL failed; retrying after a short pause")
            await asyncio.sleep(_INFRA_PAUSE_SECONDS)
        except asyncio.CancelledError:
            raise


async def async_main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, service="outbox")
    stop = asyncio.Event()
    log_event(logger, "publisher_started", pid=os.getpid())

    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        if not stop.is_set():
            log_event(logger, "publisher_stopping", pid=os.getpid())
            stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, request_stop)

    try:
        await publisher_loop(stop=stop)
    finally:
        log_event(logger, "publisher_stopped", pid=os.getpid())
        await dispose_engine()
        await dispose_redis()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        log_event(logger, "publisher_stopped")
