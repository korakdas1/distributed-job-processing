"""Worker list API schemas. Status is derived; it is not a PostgreSQL column."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

from job_platform.core.enums import WorkerLivenessStatus


class WorkerRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    hostname: str
    pid: int
    started_at: datetime
    last_seen_at: datetime
    stopped_at: datetime | None
    status: WorkerLivenessStatus
    is_alive: bool | None
    heartbeat_at: datetime | None
    heartbeat_ttl_ms: int | None


class WorkerListResponse(BaseModel):
    items: list[WorkerRead]
    total: int
    liveness_available: bool
    limit: int
    offset: int
