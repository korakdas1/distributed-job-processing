"""Load worker history from PostgreSQL and overlay Redis TTL liveness."""

from __future__ import annotations

import logging

from redis.exceptions import RedisError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from job_platform.core.enums import WorkerLivenessStatus
from job_platform.core.logging import log_event
from job_platform.models.worker import Worker
from job_platform.schemas.worker import WorkerListResponse, WorkerRead
from job_platform.worker.heartbeat import read_heartbeats
from job_platform.worker.liveness import HeartbeatView, derive_worker_liveness

logger = logging.getLogger(__name__)


def _to_read(
    row: Worker,
    *,
    redis_available: bool,
    heartbeat: HeartbeatView | None,
) -> WorkerRead:
    status, is_alive = derive_worker_liveness(
        stopped_at=row.stopped_at,
        redis_available=redis_available,
        heartbeat=heartbeat,
    )
    heartbeat_at = None
    heartbeat_ttl_ms = None
    if status is WorkerLivenessStatus.ACTIVE and heartbeat is not None:
        heartbeat_at = heartbeat.heartbeat_at
        heartbeat_ttl_ms = heartbeat.heartbeat_ttl_ms
    return WorkerRead(
        id=row.id,
        hostname=row.hostname,
        pid=row.pid,
        started_at=row.started_at,
        last_seen_at=row.last_seen_at,
        stopped_at=row.stopped_at,
        status=status,
        is_alive=is_alive,
        heartbeat_at=heartbeat_at,
        heartbeat_ttl_ms=heartbeat_ttl_ms,
    )


async def list_workers(
    session: AsyncSession,
    *,
    limit: int,
    offset: int,
) -> WorkerListResponse:
    total = int(await session.scalar(select(func.count()).select_from(Worker)) or 0)
    result = await session.execute(
        select(Worker)
        .order_by(Worker.started_at.desc(), Worker.id.desc())
        .limit(limit)
        .offset(offset)
    )
    rows = list(result.scalars().all())
    worker_ids = [row.id for row in rows]
    redis_available = True
    views: dict[str, HeartbeatView] = {}
    try:
        batch = await read_heartbeats(worker_ids)
        redis_available = batch.available
        views = batch.views
    except RedisError:
        redis_available = False
        log_event(logger, "worker_liveness_unknown", worker_count=len(worker_ids))
        logger.exception("Redis liveness lookup failed")
    else:
        if not redis_available:
            log_event(logger, "worker_liveness_unknown", worker_count=len(worker_ids))

    items: list[WorkerRead] = []
    for row in rows:
        heartbeat = views.get(row.id) if redis_available else None
        if heartbeat is not None and heartbeat.nonexpiring:
            log_event(logger, "worker_heartbeat_nonexpiring", worker_id=row.id)
        elif heartbeat is not None and heartbeat.identity_mismatch:
            log_event(logger, "worker_heartbeat_identity_mismatch", worker_id=row.id)
        elif heartbeat is not None and heartbeat.malformed:
            log_event(logger, "worker_heartbeat_malformed", worker_id=row.id)
        items.append(_to_read(row, redis_available=redis_available, heartbeat=heartbeat))
    return WorkerListResponse(
        items=items,
        total=total,
        liveness_available=redis_available,
        limit=limit,
        offset=offset,
    )
