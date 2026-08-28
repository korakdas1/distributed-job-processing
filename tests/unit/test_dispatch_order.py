from unittest.mock import AsyncMock, MagicMock

import pytest

from job_platform.api.services import create_job_record, persist_job_with_outbox
from job_platform.core.config import Settings
from job_platform.core.enums import JobPriority
from job_platform.models.job import Job
from job_platform.models.outbox import OUTBOX_EVENT_JOB_DISPATCH


def _settings() -> Settings:
    return Settings(
        postgres_password="unused",
        retry_max_attempts=5,
        job_lease_timeout_seconds=60,
        payload_max_bytes=32 * 1024,
    )


def _job() -> Job:
    return create_job_record(
        settings=_settings(),
        job_type="word_count",
        payload={"text": "hello"},
        priority=JobPriority.NORMAL,
        max_attempts=None,
        timeout_seconds=None,
        delay_seconds=0,
    )


@pytest.mark.asyncio
async def test_job_and_outbox_commit_together_without_redis() -> None:
    order: list[str] = []
    added: list[object] = []
    session = AsyncMock()

    def add(item: object) -> None:
        added.append(item)
        order.append("add")

    session.add = add

    async def flush() -> None:
        order.append("flush")

    async def commit() -> None:
        order.append("commit")

    session.flush = flush
    session.commit = commit

    job, event = await persist_job_with_outbox(session, _job())
    assert order == ["add", "add", "flush", "commit"]
    assert len(added) == 2
    assert event.job_id == job.id
    assert event.event_type == OUTBOX_EVENT_JOB_DISPATCH
    assert event.published_at is None
    assert event.redis_message_id is None
    assert event.publish_attempts == 0


@pytest.mark.asyncio
async def test_failed_commit_does_not_publish() -> None:
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock(side_effect=RuntimeError("db down"))

    with pytest.raises(RuntimeError, match="db down"):
        await persist_job_with_outbox(session, _job())
    session.flush.assert_awaited()
    session.commit.assert_awaited()
