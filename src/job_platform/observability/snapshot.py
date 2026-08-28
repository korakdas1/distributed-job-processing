"""Scrape-time operational snapshot. Observes PostgreSQL and Redis independently.

Must not mutate jobs, outbox, streams, delayed members, or worker rows.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

from redis.exceptions import RedisError, ResponseError
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from job_platform.core.clock import utcnow
from job_platform.core.config import get_settings
from job_platform.core.enums import (
    AttemptStatus,
    JobPriority,
    JobStatus,
    OperationalStatus,
    WorkerLivenessStatus,
)
from job_platform.core.logging import log_event
from job_platform.db.session import get_session_factory
from job_platform.models.job import Job, JobAttempt
from job_platform.models.outbox import (
    OUTBOX_EVENT_JOB_DEAD_LETTER,
    OUTBOX_EVENT_JOB_DISPATCH,
    OUTBOX_EVENT_JOB_INITIAL_SCHEDULE,
    OUTBOX_EVENT_JOB_RETRY_SCHEDULE,
    OutboxEvent,
)
from job_platform.models.worker import Worker
from job_platform.queue.client import get_redis
from job_platform.queue.priority import ready_streams
from job_platform.worker.heartbeat import read_heartbeats
from job_platform.worker.liveness import derive_worker_liveness

logger = logging.getLogger(__name__)

STREAM_LABELS: tuple[str, str, str, str] = ("critical", "high", "normal", "low")
KNOWN_OUTBOX_TYPES: tuple[str, ...] = (
    OUTBOX_EVENT_JOB_DISPATCH,
    OUTBOX_EVENT_JOB_INITIAL_SCHEDULE,
    OUTBOX_EVENT_JOB_RETRY_SCHEDULE,
    OUTBOX_EVENT_JOB_DEAD_LETTER,
)
UNKNOWN_OUTBOX_TYPE = "UNKNOWN"


def zero_status_counts() -> dict[str, int]:
    return {item.value: 0 for item in JobStatus}


def zero_priority_counts() -> dict[str, int]:
    return {item.value: 0 for item in JobPriority}


def zero_attempt_counts() -> dict[str, int]:
    return {item.value: 0 for item in AttemptStatus}


def zero_worker_counts() -> dict[str, int]:
    return {item.value: 0 for item in WorkerLivenessStatus}


def zero_outbox_type_counts() -> dict[str, int]:
    counts = {item: 0 for item in KNOWN_OUTBOX_TYPES}
    counts[UNKNOWN_OUTBOX_TYPE] = 0
    return counts


def operational_status(*, postgres: bool, redis: bool) -> OperationalStatus:
    if not postgres:
        return OperationalStatus.UNAVAILABLE
    if not redis:
        return OperationalStatus.DEGRADED
    return OperationalStatus.HEALTHY


def age_seconds(now: datetime, stamp: datetime | None) -> float:
    if stamp is None:
        return 0.0
    return max(0.0, (now - stamp).total_seconds())


@dataclass
class StreamCounts:
    length: int
    pending: int


@dataclass
class PostgresSnapshot:
    available: bool
    jobs_total: int | None = None
    jobs_by_status: dict[str, int] | None = None
    jobs_by_priority: dict[str, int] | None = None
    attempts_total: int | None = None
    attempts_by_status: dict[str, int] | None = None
    outbox_unpublished: int | None = None
    outbox_oldest_unpublished_age_seconds: float | None = None
    outbox_unpublished_by_type: dict[str, int] | None = None
    oldest_queued_age_seconds: float | None = None
    oldest_running_age_seconds: float | None = None
    workers_total: int | None = None
    worker_rows: list[tuple[str, datetime | None]] = field(default_factory=list)


@dataclass
class RedisSnapshot:
    available: bool
    streams: dict[str, StreamCounts] | None = None
    delayed_jobs: int | None = None
    delayed_due: int | None = None
    dead_letter_length: int | None = None


@dataclass
class WorkerLivenessSnapshot:
    liveness_available: bool
    by_status: dict[str, int]


@dataclass
class SystemSnapshot:
    generated_at: datetime
    postgres: PostgresSnapshot
    redis: RedisSnapshot
    workers: WorkerLivenessSnapshot | None


async def collect_system_snapshot() -> SystemSnapshot:
    generated_at = utcnow()
    postgres = await _collect_postgres(generated_at)
    redis = await _collect_redis(generated_at)
    workers: WorkerLivenessSnapshot | None
    if not postgres.available:
        workers = None
    elif not redis.available:
        workers = _workers_without_redis(postgres)
    else:
        workers = await _workers_with_redis(postgres)
    return SystemSnapshot(
        generated_at=generated_at,
        postgres=postgres,
        redis=redis,
        workers=workers,
    )


async def _collect_postgres(now: datetime) -> PostgresSnapshot:
    try:
        factory = get_session_factory()
        async with factory() as session:
            jobs_total = int(await session.scalar(select(func.count()).select_from(Job)) or 0)
            status_counts = zero_status_counts()
            status_rows = await session.execute(
                select(Job.status, func.count()).group_by(Job.status)
            )
            for status, count in status_rows.all():
                if status in status_counts:
                    status_counts[str(status)] = int(count)
            priority_counts = zero_priority_counts()
            priority_rows = await session.execute(
                select(Job.priority, func.count()).group_by(Job.priority)
            )
            for priority, count in priority_rows.all():
                if priority in priority_counts:
                    priority_counts[str(priority)] = int(count)
            attempts_total = int(
                await session.scalar(select(func.count()).select_from(JobAttempt)) or 0
            )
            attempt_counts = zero_attempt_counts()
            attempt_rows = await session.execute(
                select(JobAttempt.status, func.count()).group_by(JobAttempt.status)
            )
            for status, count in attempt_rows.all():
                if status in attempt_counts:
                    attempt_counts[str(status)] = int(count)
            unpublished = int(
                await session.scalar(
                    select(func.count())
                    .select_from(OutboxEvent)
                    .where(OutboxEvent.published_at.is_(None))
                )
                or 0
            )
            oldest_unpublished = await session.scalar(
                select(func.min(OutboxEvent.created_at)).where(OutboxEvent.published_at.is_(None))
            )
            type_counts = zero_outbox_type_counts()
            type_rows = await session.execute(
                select(OutboxEvent.event_type, func.count())
                .where(OutboxEvent.published_at.is_(None))
                .group_by(OutboxEvent.event_type)
            )
            for event_type, count in type_rows.all():
                key = str(event_type)
                if key in type_counts:
                    type_counts[key] += int(count)
                else:
                    type_counts[UNKNOWN_OUTBOX_TYPE] += int(count)
            oldest_queued = await session.scalar(
                select(func.min(Job.queued_at)).where(Job.status == JobStatus.QUEUED.value)
            )
            oldest_running = await session.scalar(
                select(func.min(Job.started_at)).where(Job.status == JobStatus.RUNNING.value)
            )
            worker_rows = list((await session.execute(select(Worker.id, Worker.stopped_at))).all())
            return PostgresSnapshot(
                available=True,
                jobs_total=jobs_total,
                jobs_by_status=status_counts,
                jobs_by_priority=priority_counts,
                attempts_total=attempts_total,
                attempts_by_status=attempt_counts,
                outbox_unpublished=unpublished,
                outbox_oldest_unpublished_age_seconds=age_seconds(now, oldest_unpublished),
                outbox_unpublished_by_type=type_counts,
                oldest_queued_age_seconds=age_seconds(now, oldest_queued),
                oldest_running_age_seconds=age_seconds(now, oldest_running),
                workers_total=len(worker_rows),
                worker_rows=[(str(row_id), stopped_at) for row_id, stopped_at in worker_rows],
            )
    except SQLAlchemyError:
        log_event(logger, "metrics_postgres_unavailable", level=logging.WARNING)
        return PostgresSnapshot(available=False)


async def _collect_redis(now: datetime) -> RedisSnapshot:
    settings = get_settings()
    redis = get_redis()
    try:
        await redis.ping()
    except RedisError:
        log_event(logger, "metrics_redis_unavailable", level=logging.WARNING)
        return RedisSnapshot(available=False)
    streams = ready_streams(settings)
    delayed_key = settings.redis_delayed_zset
    dead_key = settings.redis_dead_letter_stream
    group = settings.redis_consumer_group
    pipe = redis.pipeline(transaction=False)
    for stream in streams:
        pipe.xlen(stream)
    pipe.xlen(dead_key)
    pipe.zcard(delayed_key)
    pipe.zcount(delayed_key, "-inf", now.timestamp())
    try:
        results = await pipe.execute()
    except RedisError:
        log_event(logger, "metrics_redis_unavailable", level=logging.WARNING)
        return RedisSnapshot(available=False)
    stream_counts: dict[str, StreamCounts] = {}
    for label, stream, length_raw in zip(STREAM_LABELS, streams, results[:4], strict=True):
        pending = await _pending_or_zero(stream, group)
        if pending is None:
            log_event(logger, "metrics_redis_unavailable", level=logging.WARNING)
            return RedisSnapshot(available=False)
        stream_counts[label] = StreamCounts(length=int(length_raw or 0), pending=pending)
    return RedisSnapshot(
        available=True,
        streams=stream_counts,
        dead_letter_length=int(results[4] or 0),
        delayed_jobs=int(results[5] or 0),
        delayed_due=int(results[6] or 0),
    )


async def _pending_or_zero(stream: str, group: str) -> int | None:
    try:
        info = await get_redis().xpending(stream, group)
    except ResponseError as exc:
        if "NOGROUP" in str(exc):
            return 0
        log_event(logger, "metrics_redis_unavailable", level=logging.WARNING)
        return None
    except RedisError:
        return None
    return _pending_from_result(info)


def _pending_from_result(info: object) -> int | None:
    if info is None:
        return 0
    if isinstance(info, dict):
        return int(info.get("pending", 0))
    if isinstance(info, (list, tuple)) and info:
        return int(info[0])
    return None


def _workers_without_redis(postgres: PostgresSnapshot) -> WorkerLivenessSnapshot:
    counts = zero_worker_counts()
    for _worker_id, stopped_at in postgres.worker_rows:
        status, _ = derive_worker_liveness(
            stopped_at=stopped_at,
            redis_available=False,
            heartbeat=None,
        )
        counts[status.value] += 1
    return WorkerLivenessSnapshot(liveness_available=False, by_status=counts)


async def _workers_with_redis(postgres: PostgresSnapshot) -> WorkerLivenessSnapshot:
    counts = zero_worker_counts()
    worker_ids = [worker_id for worker_id, _stopped in postgres.worker_rows]
    batch = await read_heartbeats(worker_ids)
    if not batch.available:
        return _workers_without_redis(postgres)
    for worker_id, stopped_at in postgres.worker_rows:
        heartbeat = batch.views.get(worker_id)
        status, _ = derive_worker_liveness(
            stopped_at=stopped_at,
            redis_available=True,
            heartbeat=heartbeat,
        )
        counts[status.value] += 1
    return WorkerLivenessSnapshot(liveness_available=True, by_status=counts)
