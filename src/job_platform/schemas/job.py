"""Pydantic request and response schemas for jobs."""

from __future__ import annotations

import math
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from job_platform.core.enums import JobPriority, JobStatus, JobType


class JobCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: JobPriority = JobPriority.NORMAL
    max_attempts: int | None = Field(default=None, ge=1)
    timeout_seconds: int | None = Field(default=None, gt=0)
    delay_seconds: float = Field(default=0, ge=0)

    @field_validator("delay_seconds")
    @classmethod
    def delay_must_be_finite(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("delay_seconds must be a finite number")
        return value


class JobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    job_type: JobType
    status: JobStatus
    priority: JobPriority
    payload: dict[str, Any]
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    attempt_count: int
    max_attempts: int
    created_at: datetime
    queued_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    cancel_requested_at: datetime | None
    cancelled_at: datetime | None
    next_retry_at: datetime | None
    run_after: datetime | None
    worker_id: str | None
    timeout_seconds: int


class JobListResponse(BaseModel):
    items: list[JobRead]
    limit: int
    offset: int
    total: int
