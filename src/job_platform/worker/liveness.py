"""Derived worker liveness. PostgreSQL stores history; Redis TTL is current presence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from job_platform.core.enums import WorkerLivenessStatus


@dataclass(frozen=True)
class HeartbeatView:
    """Parsed Redis overlay for one worker. redis_available is process-wide."""

    present: bool
    malformed: bool = False
    nonexpiring: bool = False
    identity_mismatch: bool = False
    heartbeat_at: datetime | None = None
    heartbeat_ttl_ms: int | None = None


def derive_worker_liveness(
    *,
    stopped_at: datetime | None,
    redis_available: bool,
    heartbeat: HeartbeatView | None,
) -> tuple[WorkerLivenessStatus, bool | None]:
    """Return (status, is_alive). STOPPED wins over a leftover heartbeat key.

    A valid heartbeat is ACTIVE only when Redis is reachable, the payload is
    trustworthy, and PTTL is greater than zero. PTTL -1 (no expiry) is UNKNOWN.
    """
    if stopped_at is not None:
        return WorkerLivenessStatus.STOPPED, False
    if not redis_available:
        return WorkerLivenessStatus.UNKNOWN, None
    if heartbeat is None:
        return WorkerLivenessStatus.UNKNOWN, None
    if heartbeat.malformed or heartbeat.nonexpiring or heartbeat.identity_mismatch:
        return WorkerLivenessStatus.UNKNOWN, None
    if heartbeat.present:
        return WorkerLivenessStatus.ACTIVE, True
    return WorkerLivenessStatus.EXPIRED, False
