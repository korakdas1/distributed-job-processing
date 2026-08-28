"""Cooperative cancellation checkpoints. PostgreSQL is authoritative."""

from __future__ import annotations

import logging
import time
import uuid

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from job_platform.core.config import get_settings
from job_platform.core.logging import log_event
from job_platform.db.session import get_session_factory
from job_platform.models.job import Job
from job_platform.tasks.errors import TaskCancelledError

logger = logging.getLogger(__name__)


class TaskExecutionContext:
    """Throttled cancellation probe. Does not hold a transaction across the handler."""

    def __init__(self, job_id: uuid.UUID, *, poll_interval_ms: int | None = None) -> None:
        self.job_id = job_id
        interval = (
            poll_interval_ms
            if poll_interval_ms is not None
            else get_settings().worker_cancellation_poll_interval_ms
        )
        self._interval_s = max(0.05, interval / 1000.0)
        self._last_check = 0.0

    @property
    def sleep_chunk_seconds(self) -> float:
        return self._interval_s

    async def checkpoint(self) -> None:
        now = time.monotonic()
        if self._last_check > 0.0 and (now - self._last_check) < self._interval_s:
            return
        self._last_check = now
        try:
            requested = await probe_cancel_requested(self.job_id)
        except SQLAlchemyError:
            log_event(
                logger,
                "cancellation_probe_unavailable",
                job_id=self.job_id,
                level=logging.WARNING,
            )
            return
        if requested:
            raise TaskCancelledError()


class NullTaskContext(TaskExecutionContext):
    """Never queries PostgreSQL. Used by unit tests of handlers."""

    def __init__(self) -> None:
        self.job_id = uuid.UUID(int=0)
        self._interval_s = 0.05
        self._last_check = 0.0

    async def checkpoint(self) -> None:
        return


async def probe_cancel_requested(job_id: uuid.UUID) -> bool:
    factory = get_session_factory()
    async with factory() as session:
        row = (
            await session.execute(select(Job.cancel_requested_at).where(Job.id == job_id))
        ).one_or_none()
    if row is None:
        return False
    return row[0] is not None
