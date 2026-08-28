"""Independent worker process loop. One job at a time. Not started by FastAPI.

Startup: register in PostgreSQL, ensure all four consumer groups, write an
initial Redis TTL heartbeat, then emit worker_ready. A background heartbeat
task continues while a job runs and while SIGTERM drain finishes. Heartbeat
does not drive crash-recovery reclaim.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import time
import uuid
from datetime import datetime

from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from job_platform.core.clock import utcnow
from job_platform.core.config import get_settings
from job_platform.core.logging import configure_logging, log_event
from job_platform.db.session import dispose_engine
from job_platform.queue.client import dispose_redis
from job_platform.queue.priority import WeightedPrioritySelector, ready_streams
from job_platform.queue.streams import (
    StreamMessage,
    autoclaim_stale,
    ensure_consumer_groups,
    read_one_from_stream,
)
from job_platform.worker.heartbeat import delete_heartbeat, heartbeat_loop, write_heartbeat
from job_platform.worker.processor import process_message
from job_platform.worker.registry import (
    WorkerIdentityConflict,
    mark_worker_stopped,
    register_worker,
)

logger = logging.getLogger(__name__)

_INFRA_PAUSE_SECONDS = 1.0


def generate_worker_id() -> str:
    return f"worker-{uuid.uuid4().hex[:8]}"


async def _register_with_retry(
    worker_id: str,
    *,
    started_at: datetime,
    hostname: str,
    pid: int,
    stop: asyncio.Event | None,
) -> bool:
    while stop is None or not stop.is_set():
        try:
            await register_worker(
                worker_id=worker_id,
                started_at=started_at,
                hostname=hostname,
                pid=pid,
            )
            return True
        except WorkerIdentityConflict:
            log_event(logger, "worker_registration_conflict", worker_id=worker_id, pid=pid)
            logger.exception("Worker id collision; refusing to reuse another process row")
            raise
        except SQLAlchemyError:
            log_event(logger, "worker_registration_failed", worker_id=worker_id, pid=pid)
            logger.exception("PostgreSQL registration failed; retrying after a short pause")
            await asyncio.sleep(_INFRA_PAUSE_SECONDS)
        except asyncio.CancelledError:
            raise
    return False


async def _ensure_groups_with_retry(worker_id: str, stop: asyncio.Event | None) -> bool:
    """Retry consumer-group creation until Redis is available. Returns False if stopped."""
    while stop is None or not stop.is_set():
        try:
            await ensure_consumer_groups()
            return True
        except RedisError:
            log_event(logger, "worker_queue_init_failed", worker_id=worker_id, pid=os.getpid())
            logger.exception("Consumer-group initialization failed; retrying after a short pause")
            await asyncio.sleep(_INFRA_PAUSE_SECONDS)
        except asyncio.CancelledError:
            raise
    return False


async def _initial_heartbeat_with_retry(worker_id: str, stop: asyncio.Event | None) -> bool:
    while stop is None or not stop.is_set():
        try:
            await write_heartbeat(worker_id)
            return True
        except RedisError:
            log_event(logger, "worker_heartbeat_failed", worker_id=worker_id)
            logger.exception("Initial Redis heartbeat failed; retrying after a short pause")
            await asyncio.sleep(_INFRA_PAUSE_SECONDS)
        except asyncio.CancelledError:
            raise
    return False


async def _reclaim_one(
    worker_id: str,
    *,
    min_idle_ms: int,
    reclaim_index: int,
) -> tuple[StreamMessage | None, int]:
    streams = ready_streams()
    count = len(streams)
    for offset in range(count):
        index = (reclaim_index + offset) % count
        stream = streams[index]
        reclaimed = await autoclaim_stale(stream, worker_id, min_idle_ms=min_idle_ms, count=1)
        if reclaimed is not None:
            log_event(
                logger,
                "priority_reclaim",
                worker_id=worker_id,
                stream=reclaimed.stream,
                job_id=reclaimed.job_id,
                message_id=reclaimed.message_id,
            )
            return reclaimed, (index + 1) % count
    return None, (reclaim_index + 1) % count


async def _read_fair(
    worker_id: str,
    selector: WeightedPrioritySelector,
) -> StreamMessage | None:
    """Non-blocking probe of credited streams. Empty high-priority streams are skipped."""
    probed_empty: set[str] = set()
    stream_count = len(ready_streams())
    for _ in range(stream_count * 2):
        remaining = selector.streams_with_credit()
        if remaining and remaining.issubset(probed_empty):
            if selector.has_zero_credit():
                selector.reset()
                probed_empty.clear()
                continue
            return None
        stream = selector.next_stream()
        message = await read_one_from_stream(stream, worker_id, block_ms=0)
        if message is not None:
            selector.record_consumed(stream)
            log_event(
                logger,
                "priority_selected",
                worker_id=worker_id,
                stream=stream,
                job_id=message.job_id,
                message_id=message.message_id,
            )
            return message
        selector.skip_empty(stream)
        probed_empty.add(stream)
    return None


async def worker_loop(
    worker_id: str,
    *,
    stop: asyncio.Event | None = None,
) -> None:
    settings = get_settings()
    selector = WeightedPrioritySelector.from_settings(settings)
    min_idle_ms = settings.job_lease_timeout_seconds * 1000
    reclaim_interval = settings.worker_reclaim_interval_ms / 1000.0
    idle_seconds = settings.worker_read_block_ms / 1000.0
    next_reclaim_at = 0.0
    reclaim_index = 0
    while stop is None or not stop.is_set():
        try:
            now = time.monotonic()
            if now >= next_reclaim_at:
                next_reclaim_at = now + reclaim_interval
                reclaimed, reclaim_index = await _reclaim_one(
                    worker_id,
                    min_idle_ms=min_idle_ms,
                    reclaim_index=reclaim_index,
                )
                if reclaimed is not None:
                    log_event(
                        logger,
                        "message_received",
                        job_id=reclaimed.job_id,
                        worker_id=worker_id,
                        stream=reclaimed.stream,
                        message_id=reclaimed.message_id,
                    )
                    await process_message(worker_id, reclaimed, reclaimed=True)
                    continue
            message = await _read_fair(worker_id, selector)
            if stop is not None and stop.is_set() and message is None:
                break
            if message is None:
                await asyncio.sleep(idle_seconds)
                continue
            log_event(
                logger,
                "message_received",
                job_id=message.job_id,
                worker_id=worker_id,
                stream=message.stream,
                message_id=message.message_id,
            )
            await process_message(worker_id, message)
        except RedisError:
            log_event(logger, "queue_error", worker_id=worker_id)
            logger.exception("Redis read/process failed; retrying after a short pause")
            await asyncio.sleep(_INFRA_PAUSE_SECONDS)
        except SQLAlchemyError:
            log_event(logger, "database_error", worker_id=worker_id)
            logger.exception("PostgreSQL failed; retrying after a short pause")
            await asyncio.sleep(_INFRA_PAUSE_SECONDS)
        except asyncio.CancelledError:
            raise


async def _heartbeat_supervisor(
    worker_id: str,
    heartbeat_stop: asyncio.Event,
    claim_stop: asyncio.Event,
) -> None:
    try:
        await heartbeat_loop(worker_id, stop=heartbeat_stop)
    except asyncio.CancelledError:
        raise
    except Exception:
        log_event(logger, "worker_heartbeat_task_failed", worker_id=worker_id)
        logger.exception("Heartbeat task failed unexpectedly; stopping worker")
        claim_stop.set()
        raise


async def async_main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, service="worker")
    worker_id = generate_worker_id()
    started_at = utcnow()
    hostname = socket.gethostname()
    pid = os.getpid()
    claim_stop = asyncio.Event()
    heartbeat_stop = asyncio.Event()
    log_event(logger, "worker_started", worker_id=worker_id, pid=pid, hostname=hostname)

    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        if not claim_stop.is_set():
            log_event(logger, "worker_stopping", worker_id=worker_id, pid=pid)
            claim_stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, request_stop)

    registered = False
    heartbeat_task: asyncio.Task[None] | None = None
    heartbeat_crashed = False
    try:
        registered = await _register_with_retry(
            worker_id,
            started_at=started_at,
            hostname=hostname,
            pid=pid,
            stop=claim_stop,
        )
        if not registered:
            return
        groups_ready = await _ensure_groups_with_retry(worker_id, claim_stop)
        if not groups_ready:
            return
        heartbeat_ok = await _initial_heartbeat_with_retry(worker_id, claim_stop)
        if not heartbeat_ok:
            return
        heartbeat_task = asyncio.create_task(
            _heartbeat_supervisor(worker_id, heartbeat_stop, claim_stop),
            name=f"heartbeat-{worker_id}",
        )
        log_event(logger, "worker_ready", worker_id=worker_id, pid=pid)
        await worker_loop(worker_id, stop=claim_stop)
    finally:
        if heartbeat_task is not None and heartbeat_task.done() and not heartbeat_task.cancelled():
            heartbeat_crashed = heartbeat_task.exception() is not None
        if registered and not heartbeat_crashed:
            try:
                await mark_worker_stopped(worker_id)
            except SQLAlchemyError:
                log_event(logger, "worker_stop_register_failed", worker_id=worker_id)
                logger.exception("Failed to record worker stopped_at")
            except asyncio.CancelledError:
                raise
        heartbeat_stop.set()
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            except Exception:
                heartbeat_crashed = True
        if registered:
            try:
                await delete_heartbeat(worker_id)
                log_event(logger, "worker_heartbeat_removed", worker_id=worker_id)
            except RedisError:
                log_event(logger, "worker_heartbeat_remove_failed", worker_id=worker_id)
                logger.exception("Best-effort heartbeat delete failed")
            except asyncio.CancelledError:
                raise
        log_event(logger, "worker_stopped", worker_id=worker_id, pid=pid)
        await dispose_engine()
        await dispose_redis()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        log_event(logger, "worker_stopped")
