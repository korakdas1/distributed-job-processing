"""PostgreSQL worker registry. One row per OS process; not a liveness source."""

from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from job_platform.core.clock import utcnow
from job_platform.core.logging import log_event
from job_platform.db.session import get_session_factory
from job_platform.models.worker import Worker

logger = logging.getLogger(__name__)


class WorkerIdentityConflict(RuntimeError):
    """The worker_id already belongs to a different process. Do not reuse the row."""


async def register_worker(
    *,
    worker_id: str,
    started_at: datetime,
    hostname: str,
    pid: int,
) -> Worker:
    """Insert this process, or accept a retry of the same id/hostname/pid."""
    factory = get_session_factory()
    async with factory() as session:
        stmt = (
            insert(Worker)
            .values(
                id=worker_id,
                started_at=started_at,
                last_seen_at=started_at,
                stopped_at=None,
                hostname=hostname,
                pid=pid,
            )
            .on_conflict_do_nothing(index_elements=["id"])
        )
        await session.execute(stmt)
        await session.commit()
        row = await session.get(Worker, worker_id)
        if row is None:
            raise RuntimeError(f"Worker {worker_id} was not persisted")
        if row.hostname != hostname or row.pid != pid or row.stopped_at is not None:
            raise WorkerIdentityConflict(
                f"Worker id {worker_id} already registered as host={row.hostname} pid={row.pid}"
            )
        log_event(
            logger,
            "worker_registered",
            worker_id=worker_id,
            hostname=hostname,
            pid=pid,
        )
        return row


async def sample_worker_last_seen(worker_id: str) -> None:
    factory = get_session_factory()
    now = utcnow()
    async with factory() as session:
        await session.execute(
            update(Worker)
            .where(Worker.id == worker_id, Worker.stopped_at.is_(None))
            .values(last_seen_at=now)
        )
        await session.commit()


async def mark_worker_stopped(worker_id: str) -> None:
    factory = get_session_factory()
    now = utcnow()
    async with factory() as session:
        await session.execute(
            update(Worker).where(Worker.id == worker_id).values(stopped_at=now, last_seen_at=now)
        )
        await session.commit()
        log_event(logger, "worker_stopped_registered", worker_id=worker_id)


def is_retryable_registry_error(exc: BaseException) -> bool:
    return isinstance(exc, SQLAlchemyError)
