"""JSON schemas for GET /metrics/summary. Prometheus text is not modeled here."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from job_platform.core.enums import OperationalStatus


class DependencyAvailability(BaseModel):
    available: bool


class JobsSummary(BaseModel):
    total: int
    by_status: dict[str, int]
    by_priority: dict[str, int]
    attempts_total: int
    attempts_by_status: dict[str, int]
    oldest_queued_age_seconds: float
    oldest_running_age_seconds: float


class OutboxSummary(BaseModel):
    unpublished: int
    oldest_unpublished_age_seconds: float
    unpublished_by_type: dict[str, int]


class StreamSummary(BaseModel):
    length: int
    pending: int


class DelayedSummary(BaseModel):
    count: int
    due: int


class QueuesSummary(BaseModel):
    streams: dict[str, StreamSummary]
    delayed: DelayedSummary
    dead_letter_length: int


class WorkersSummary(BaseModel):
    total_history: int
    by_status: dict[str, int]
    liveness_available: bool


class MetricsSummaryResponse(BaseModel):
    generated_at: datetime
    status: OperationalStatus
    dependencies: dict[str, DependencyAvailability]
    jobs: JobsSummary | None
    outbox: OutboxSummary | None
    queues: QueuesSummary | None
    workers: WorkersSummary | None
