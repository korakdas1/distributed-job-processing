"""Redis sorted set jobs:delayed for time eligibility.

Members are job_id strings. PostgreSQL status decides whether a due member is
a first-run SCHEDULED job (run_after) or a RETRYING job (next_retry_at).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

from job_platform.core.config import get_settings
from job_platform.queue.client import get_redis


def _key() -> str:
    return get_settings().redis_delayed_zset


def _score(run_at: datetime) -> float:
    return run_at.timestamp()


@dataclass(frozen=True)
class DelayedMember:
    raw: str
    score: float
    job_id: uuid.UUID | None


async def schedule_delayed(job_id: uuid.UUID, run_at: datetime) -> None:
    """ZADD member=job_id score=unix timestamp. Idempotent for the same member+score."""
    await get_redis().zadd(_key(), {str(job_id): _score(run_at)})


async def due_members(now: datetime, limit: int) -> list[DelayedMember]:
    """ZRANGEBYSCORE -inf now with a batch limit. Does not pop members.

    Returns the exact Redis member string so malformed entries can be removed.
    """
    rows = await get_redis().zrangebyscore(
        _key(),
        min="-inf",
        max=_score(now),
        start=0,
        num=limit,
        withscores=True,
    )
    members: list[DelayedMember] = []
    for raw_member, score in rows:
        raw = str(raw_member)
        try:
            job_id: uuid.UUID | None = uuid.UUID(raw)
        except ValueError:
            job_id = None
        members.append(DelayedMember(raw=raw, score=float(score), job_id=job_id))
    return members


async def due_job_ids(now: datetime, limit: int) -> list[uuid.UUID]:
    """Valid UUID members only. Prefer due_members when poison cleanup is required."""
    return [item.job_id for item in await due_members(now, limit) if item.job_id is not None]


async def remove_delayed(job_id: uuid.UUID) -> None:
    await get_redis().zrem(_key(), str(job_id))


async def remove_delayed_member(raw: str) -> None:
    await get_redis().zrem(_key(), raw)


async def rescore_delayed(job_id: uuid.UUID, run_at: datetime) -> None:
    await get_redis().zadd(_key(), {str(job_id): _score(run_at)})
