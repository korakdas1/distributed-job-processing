"""Redis sorted set jobs:delayed for time eligibility.

Members remain job_id strings. A companion hash stores the scheduling outbox
ID, so equal timestamps do not conflate different scheduling rounds. Legacy
members have no ID. PostgreSQL still decides status and time eligibility.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

from job_platform.core.config import get_settings
from job_platform.queue.client import get_redis

_PUBLISH = """
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
return 1
"""

_DUE = """
local rows = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1],
                        'WITHSCORES', 'LIMIT', 0, ARGV[2])
local result = {}
for i = 1, #rows, 2 do
    table.insert(result, rows[i])
    table.insert(result, rows[i + 1])
    table.insert(result, redis.call('HGET', KEYS[2], rows[i]) or '')
end
return result
"""

# Numeric comparison preserves Redis's binary64 score across Python's repr
# round trip; string equality would reject equivalent decimal spellings.
_MATCH_OBSERVATION = """
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score or tonumber(score) ~= tonumber(ARGV[2]) then return 0 end
local generation = redis.call('HGET', KEYS[2], ARGV[1]) or ''
if generation ~= ARGV[3] then return 0 end
"""

_REMOVE = (
    _MATCH_OBSERVATION
    + """
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
"""
)

_RESCORE = (
    _MATCH_OBSERVATION
    + """
redis.call('ZADD', KEYS[1], ARGV[4], ARGV[1])
return 1
"""
)


def _key() -> str:
    return get_settings().redis_delayed_zset


def _generation_key() -> str:
    return f"{_key()}:generations"


def _score(run_at: datetime) -> float:
    return run_at.timestamp()


@dataclass(frozen=True)
class DelayedMember:
    raw: str
    score: float
    job_id: uuid.UUID | None
    generation: str | None = None


async def schedule_delayed(
    job_id: uuid.UUID, run_at: datetime, *, outbox_event_id: uuid.UUID
) -> None:
    """Publish one durable round; caller must lock and validate the current Job.

    Replaying the same outbox ID is idempotent. Ordering of different IDs comes
    from PostgreSQL, not UUID ordering or wall-clock timestamps.
    """
    script = get_redis().register_script(_PUBLISH)
    await script(
        keys=[_key(), _generation_key()],
        args=[str(job_id), _score(run_at), str(outbox_event_id)],
    )


async def due_members(now: datetime, limit: int) -> list[DelayedMember]:
    """ZRANGEBYSCORE -inf now with a batch limit. Does not pop members.

    Read member, score and generation in one atomic snapshot. A separate HGET
    could otherwise pair an old score with a newly published generation.
    """
    script = get_redis().register_script(_DUE)
    rows = await script(keys=[_key(), _generation_key()], args=[_score(now), limit])
    members: list[DelayedMember] = []
    for index in range(0, len(rows), 3):
        raw = str(rows[index])
        try:
            job_id: uuid.UUID | None = uuid.UUID(raw)
        except ValueError:
            job_id = None
        members.append(
            DelayedMember(
                raw=raw,
                score=float(rows[index + 1]),
                job_id=job_id,
                generation=str(rows[index + 2]) or None,
            )
        )
    return members


async def due_job_ids(now: datetime, limit: int) -> list[uuid.UUID]:
    """Valid UUID members only. Prefer due_members when poison cleanup is required."""
    return [item.job_id for item in await due_members(now, limit) if item.job_id is not None]


async def remove_delayed(member: DelayedMember) -> bool:
    """Remove only the observed schedule, including its companion metadata."""
    script = get_redis().register_script(_REMOVE)
    return bool(
        await script(
            keys=[_key(), _generation_key()],
            args=[member.raw, member.score, member.generation or ""],
        )
    )


async def remove_delayed_member(member: DelayedMember) -> bool:
    """The same conditional removal also handles malformed legacy members."""
    return await remove_delayed(member)


async def rescore_delayed(member: DelayedMember, run_at: datetime) -> bool:
    """Repair an observed score without replacing a newer schedule or resurrecting it."""
    script = get_redis().register_script(_RESCORE)
    return bool(
        await script(
            keys=[_key(), _generation_key()],
            args=[member.raw, member.score, member.generation or "", _score(run_at)],
        )
    )
