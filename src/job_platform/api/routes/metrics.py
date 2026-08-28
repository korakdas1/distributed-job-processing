"""Prometheus text and JSON operational summary. Observation only."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi.responses import Response

from job_platform.observability.metrics import CONTENT_TYPE, render_latest
from job_platform.observability.snapshot import collect_system_snapshot, operational_status
from job_platform.schemas.metrics import (
    DelayedSummary,
    DependencyAvailability,
    JobsSummary,
    MetricsSummaryResponse,
    OutboxSummary,
    QueuesSummary,
    StreamSummary,
    WorkersSummary,
)
from job_platform.security.dependencies import require_viewer
from job_platform.security.principal import Principal

router = APIRouter(tags=["metrics"])

ViewerDep = Annotated[Principal, Depends(require_viewer)]


@router.get("/metrics")
async def prometheus_metrics(_principal: ViewerDep) -> Response:
    body = await render_latest()
    return Response(content=body, media_type=CONTENT_TYPE)


@router.get("/metrics/summary", response_model=MetricsSummaryResponse)
async def metrics_summary(_principal: ViewerDep) -> MetricsSummaryResponse:
    snapshot = await collect_system_snapshot()
    postgres = snapshot.postgres
    redis = snapshot.redis
    jobs = None
    outbox = None
    workers = None
    if postgres.available:
        assert postgres.jobs_total is not None
        assert postgres.jobs_by_status is not None
        assert postgres.jobs_by_priority is not None
        assert postgres.attempts_total is not None
        assert postgres.attempts_by_status is not None
        assert postgres.outbox_unpublished is not None
        assert postgres.outbox_oldest_unpublished_age_seconds is not None
        assert postgres.outbox_unpublished_by_type is not None
        assert postgres.oldest_queued_age_seconds is not None
        assert postgres.oldest_running_age_seconds is not None
        assert postgres.workers_total is not None
        jobs = JobsSummary(
            total=postgres.jobs_total,
            by_status=postgres.jobs_by_status,
            by_priority=postgres.jobs_by_priority,
            attempts_total=postgres.attempts_total,
            attempts_by_status=postgres.attempts_by_status,
            oldest_queued_age_seconds=postgres.oldest_queued_age_seconds,
            oldest_running_age_seconds=postgres.oldest_running_age_seconds,
        )
        outbox = OutboxSummary(
            unpublished=postgres.outbox_unpublished,
            oldest_unpublished_age_seconds=postgres.outbox_oldest_unpublished_age_seconds,
            unpublished_by_type=postgres.outbox_unpublished_by_type,
        )
        if snapshot.workers is not None:
            workers = WorkersSummary(
                total_history=postgres.workers_total,
                by_status=snapshot.workers.by_status,
                liveness_available=snapshot.workers.liveness_available,
            )
    queues = None
    if redis.available:
        assert redis.streams is not None
        assert redis.delayed_jobs is not None
        assert redis.delayed_due is not None
        assert redis.dead_letter_length is not None
        queues = QueuesSummary(
            streams={
                label: StreamSummary(length=counts.length, pending=counts.pending)
                for label, counts in redis.streams.items()
            },
            delayed=DelayedSummary(count=redis.delayed_jobs, due=redis.delayed_due),
            dead_letter_length=redis.dead_letter_length,
        )
    return MetricsSummaryResponse(
        generated_at=snapshot.generated_at,
        status=operational_status(postgres=postgres.available, redis=redis.available),
        dependencies={
            "postgres": DependencyAvailability(available=postgres.available),
            "redis": DependencyAvailability(available=redis.available),
        },
        jobs=jobs,
        outbox=outbox,
        queues=queues,
        workers=workers,
    )
