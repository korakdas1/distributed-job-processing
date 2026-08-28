"""Redis TTL heartbeats. Observational liveness only — not a processing lease."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from job_platform.core.clock import utcnow
from job_platform.core.config import Settings, get_settings
from job_platform.core.logging import log_event
from job_platform.queue.client import get_redis
from job_platform.worker.liveness import HeartbeatView
from job_platform.worker.registry import sample_worker_last_seen

logger = logging.getLogger(__name__)


def heartbeat_key(worker_id: str) -> str:
    return f"worker:{worker_id}:heartbeat"


def heartbeat_ttl_ms(settings: Settings | None = None) -> int:
    cfg = settings if settings is not None else get_settings()
    return max(1, int(cfg.worker_heartbeat_ttl_seconds * 1000))


def parse_heartbeat_payload(raw: str, *, worker_id: str) -> datetime | None:
    """Return heartbeat_at or None when the payload is unusable."""
    classified = _classify_heartbeat_payload(raw, worker_id=worker_id)
    if classified.malformed or classified.identity_mismatch or not classified.present:
        return None
    return classified.heartbeat_at


def _classify_heartbeat_payload(raw: str, *, worker_id: str) -> HeartbeatView:
    """Parse JSON without TTL semantics. Caller applies PTTL rules."""
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return HeartbeatView(present=False, malformed=True)
    if not isinstance(data, dict):
        return HeartbeatView(present=False, malformed=True)
    declared = data.get("worker_id")
    if declared != worker_id:
        return HeartbeatView(present=False, identity_mismatch=True)
    stamp = data.get("heartbeat_at")
    if not isinstance(stamp, str):
        return HeartbeatView(present=False, malformed=True)
    try:
        parsed = datetime.fromisoformat(stamp)
    except ValueError:
        return HeartbeatView(present=False, malformed=True)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return HeartbeatView(present=False, malformed=True)
    return HeartbeatView(present=True, heartbeat_at=parsed)


def heartbeat_view_from_redis(
    *,
    worker_id: str,
    raw: str | None,
    pttl_ms: int,
) -> HeartbeatView:
    """Map GET + PTTL onto a HeartbeatView.

    PTTL -2 (or a missing value) means the key is gone. PTTL -1 means the key
    exists without expiry and must not be treated as ACTIVE. ACTIVE requires
    a trustworthy payload and PTTL > 0.
    """
    if raw is None or pttl_ms == -2:
        return HeartbeatView(present=False)
    classified = _classify_heartbeat_payload(raw, worker_id=worker_id)
    if classified.malformed or classified.identity_mismatch:
        return classified
    if pttl_ms == -1:
        return HeartbeatView(
            present=False,
            nonexpiring=True,
            heartbeat_at=classified.heartbeat_at,
        )
    if pttl_ms <= 0:
        return HeartbeatView(present=False)
    return HeartbeatView(
        present=True,
        heartbeat_at=classified.heartbeat_at,
        heartbeat_ttl_ms=pttl_ms,
    )


@dataclass(frozen=True)
class HeartbeatBatch:
    available: bool
    views: dict[str, HeartbeatView]


async def write_heartbeat(worker_id: str, *, settings: Settings | None = None) -> datetime:
    cfg = settings if settings is not None else get_settings()
    heartbeat_at = utcnow()
    payload = json.dumps(
        {"worker_id": worker_id, "heartbeat_at": heartbeat_at.isoformat()},
        separators=(",", ":"),
    )
    await get_redis().set(heartbeat_key(worker_id), payload, px=heartbeat_ttl_ms(cfg))
    return heartbeat_at


async def delete_heartbeat(worker_id: str) -> None:
    await get_redis().delete(heartbeat_key(worker_id))


async def read_heartbeats(worker_ids: list[str]) -> HeartbeatBatch:
    if not worker_ids:
        try:
            await get_redis().ping()
        except RedisError:
            return HeartbeatBatch(available=False, views={})
        return HeartbeatBatch(available=True, views={})
    redis = get_redis()
    pipe = redis.pipeline(transaction=False)
    for worker_id in worker_ids:
        pipe.get(heartbeat_key(worker_id))
        pipe.pttl(heartbeat_key(worker_id))
    try:
        results: list[Any] = await pipe.execute()
    except RedisError:
        return HeartbeatBatch(available=False, views={})
    views: dict[str, HeartbeatView] = {}
    for index, worker_id in enumerate(worker_ids):
        raw = results[index * 2]
        pttl_raw = results[index * 2 + 1]
        pttl_ms = int(pttl_raw) if pttl_raw is not None else -2
        value = str(raw) if raw is not None else None
        views[worker_id] = heartbeat_view_from_redis(
            worker_id=worker_id,
            raw=value,
            pttl_ms=pttl_ms,
        )
    return HeartbeatBatch(available=True, views=views)


async def heartbeat_loop(
    worker_id: str,
    *,
    stop: asyncio.Event,
    settings: Settings | None = None,
) -> None:
    """Refresh Redis TTL and sample PostgreSQL last_seen_at until stop is set."""
    cfg = settings if settings is not None else get_settings()
    interval = cfg.worker_heartbeat_interval_seconds
    sample_interval = cfg.worker_db_heartbeat_interval_seconds
    next_sample_at = time.monotonic() + sample_interval
    redis_outage = False
    log_event(logger, "worker_heartbeat_started", worker_id=worker_id)
    while not stop.is_set():
        try:
            await write_heartbeat(worker_id, settings=cfg)
            if redis_outage:
                log_event(logger, "worker_heartbeat_restored", worker_id=worker_id)
                redis_outage = False
            logger.debug("event=worker_heartbeat_ok worker_id=%s", worker_id)
            now = time.monotonic()
            if now >= next_sample_at:
                next_sample_at = now + sample_interval
                try:
                    await sample_worker_last_seen(worker_id)
                    logger.debug("event=worker_last_seen_sampled worker_id=%s", worker_id)
                except SQLAlchemyError:
                    log_event(logger, "worker_last_seen_failed", worker_id=worker_id)
                    logger.exception("PostgreSQL last_seen sample failed; Redis heartbeat kept")
        except RedisError:
            if not redis_outage:
                log_event(logger, "worker_heartbeat_failed", worker_id=worker_id)
                logger.exception("Redis heartbeat write failed; will retry")
                redis_outage = True
        except asyncio.CancelledError:
            raise
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue
        except asyncio.CancelledError:
            raise
