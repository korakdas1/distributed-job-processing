"""Prometheus metric registry. Process-local HTTP metrics plus scrape-time gauges.

HTTP counters/histograms describe this API process and reset on restart.
System gauges are rebuilt from PostgreSQL/Redis on each scrape. Unavailable
dependency-derived gauges are set to NaN so stale values cannot look current.
"""

from __future__ import annotations

import asyncio
from typing import Final

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from prometheus_client.gc_collector import GCCollector
from prometheus_client.platform_collector import PlatformCollector
from prometheus_client.process_collector import ProcessCollector

from job_platform.core.enums import (
    AttemptStatus,
    JobPriority,
    JobStatus,
    WorkerLivenessStatus,
)
from job_platform.observability.snapshot import (
    KNOWN_OUTBOX_TYPES,
    STREAM_LABELS,
    UNKNOWN_OUTBOX_TYPE,
    SystemSnapshot,
    collect_system_snapshot,
)

CONTENT_TYPE: Final[str] = CONTENT_TYPE_LATEST
_NAN = float("nan")
_HTTP_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)
_AUTH_FAILURE_REASONS = ("missing", "invalid")
_AUTHZ_ROLES = ("viewer", "operator", "anonymous")
_LIMIT_CLASSES = ("read", "write")

REGISTRY = CollectorRegistry()
ProcessCollector(registry=REGISTRY)
PlatformCollector(registry=REGISTRY)
GCCollector(registry=REGISTRY)

HTTP_REQUESTS = Counter(
    "job_platform_http_requests_total",
    "HTTP requests handled by this API process.",
    ["method", "route", "status_code"],
    registry=REGISTRY,
)
HTTP_DURATION = Histogram(
    "job_platform_http_request_duration_seconds",
    "HTTP request duration in seconds for this API process.",
    ["method", "route"],
    buckets=_HTTP_BUCKETS,
    registry=REGISTRY,
)
AUTH_FAILURES = Counter(
    "job_platform_auth_failures_total",
    "Authentication failures (missing or invalid API key).",
    ["reason"],
    registry=REGISTRY,
)
AUTHZ_DENIED = Counter(
    "job_platform_authorization_denied_total",
    "Authorization denials after a valid API key was presented.",
    ["role"],
    registry=REGISTRY,
)
RATE_LIMIT_REJECTS = Counter(
    "job_platform_rate_limit_rejected_total",
    "Requests rejected by the process-local rate limiter.",
    ["limit_class"],
    registry=REGISTRY,
)
DEPENDENCY_UP = Gauge(
    "job_platform_dependency_up",
    "1 if the named dependency is reachable from this API process, else 0.",
    ["dependency"],
    registry=REGISTRY,
)
JOBS = Gauge(
    "job_platform_jobs",
    "Current durable job rows grouped by PostgreSQL status.",
    ["status"],
    registry=REGISTRY,
)
JOBS_TOTAL = Gauge(
    "job_platform_jobs_total",
    "Current number of durable job rows in PostgreSQL.",
    registry=REGISTRY,
)
JOBS_BY_PRIORITY = Gauge(
    "job_platform_jobs_by_priority",
    "Current durable job rows grouped by stored priority.",
    ["priority"],
    registry=REGISTRY,
)
JOB_ATTEMPTS_TOTAL = Gauge(
    "job_platform_job_attempts_total",
    "Current number of durable job_attempts rows in PostgreSQL.",
    registry=REGISTRY,
)
JOB_ATTEMPTS = Gauge(
    "job_platform_job_attempts",
    "Current durable job_attempts rows grouped by attempt status.",
    ["status"],
    registry=REGISTRY,
)
OUTBOX_UNPUBLISHED = Gauge(
    "job_platform_outbox_unpublished",
    "Outbox events whose published_at is still null.",
    registry=REGISTRY,
)
OUTBOX_OLDEST_AGE = Gauge(
    "job_platform_outbox_oldest_unpublished_age_seconds",
    "Age in seconds of the oldest unpublished outbox event; 0 if none.",
    registry=REGISTRY,
)
OUTBOX_UNPUBLISHED_BY_TYPE = Gauge(
    "job_platform_outbox_unpublished_by_type",
    "Unpublished outbox events grouped by bounded event_type labels.",
    ["event_type"],
    registry=REGISTRY,
)
REDIS_STREAM_LENGTH = Gauge(
    "job_platform_redis_stream_length",
    "Redis XLEN for a ready stream. Retained entries, not exact outstanding jobs.",
    ["stream"],
    registry=REGISTRY,
)
REDIS_PENDING = Gauge(
    "job_platform_redis_pending",
    "Consumer-group pending (delivered, not yet acknowledged) entries per ready stream.",
    ["stream"],
    registry=REGISTRY,
)
DELAYED_JOBS = Gauge(
    "job_platform_delayed_jobs",
    "ZCARD of jobs:delayed (first-run SCHEDULED and RETRYING members).",
    registry=REGISTRY,
)
DELAYED_DUE = Gauge(
    "job_platform_delayed_due",
    "ZCOUNT of jobs:delayed members whose score is due now.",
    registry=REGISTRY,
)
DEAD_LETTER_LENGTH = Gauge(
    "job_platform_dead_letter_stream_length",
    "XLEN of jobs:dead. Operational copies; duplicates are possible.",
    registry=REGISTRY,
)
WORKERS = Gauge(
    "job_platform_workers",
    "Worker-history rows grouped by derived liveness status.",
    ["status"],
    registry=REGISTRY,
)
WORKERS_TOTAL = Gauge(
    "job_platform_workers_total",
    "Current number of durable workers history rows in PostgreSQL.",
    registry=REGISTRY,
)
OLDEST_QUEUED_AGE = Gauge(
    "job_platform_oldest_queued_age_seconds",
    "Age in seconds of the oldest QUEUED job; 0 if none.",
    registry=REGISTRY,
)
OLDEST_RUNNING_AGE = Gauge(
    "job_platform_oldest_running_age_seconds",
    "Age in seconds of the oldest RUNNING job; 0 if none.",
    registry=REGISTRY,
)

_LOCK = asyncio.Lock()


def observe_http_request(
    *,
    method: str,
    route: str,
    status_code: int,
    duration_seconds: float,
) -> None:
    HTTP_REQUESTS.labels(method=method, route=route, status_code=str(status_code)).inc()
    HTTP_DURATION.labels(method=method, route=route).observe(duration_seconds)


def observe_auth_failure(reason: str) -> None:
    label = reason if reason in _AUTH_FAILURE_REASONS else "invalid"
    AUTH_FAILURES.labels(reason=label).inc()


def observe_authorization_denied(role: str) -> None:
    label = role if role in _AUTHZ_ROLES else "anonymous"
    AUTHZ_DENIED.labels(role=label).inc()


def observe_rate_limited(limit_class: str) -> None:
    label = limit_class if limit_class in _LIMIT_CLASSES else "write"
    RATE_LIMIT_REJECTS.labels(limit_class=label).inc()


def _set_or_nan(gauge: Gauge, value: float | None) -> None:
    gauge.set(_NAN if value is None else value)


def _set_labeled_or_nan(gauge: Gauge, label: str, key: str, values: dict[str, int] | None) -> None:
    if values is None:
        gauge.labels(**{label: key}).set(_NAN)
        return
    gauge.labels(**{label: key}).set(float(values.get(key, 0)))


def apply_snapshot(snapshot: SystemSnapshot) -> None:
    postgres = snapshot.postgres
    redis = snapshot.redis
    DEPENDENCY_UP.labels(dependency="postgres").set(1 if postgres.available else 0)
    DEPENDENCY_UP.labels(dependency="redis").set(1 if redis.available else 0)

    _set_or_nan(JOBS_TOTAL, None if postgres.jobs_total is None else float(postgres.jobs_total))
    for job_status in JobStatus:
        _set_labeled_or_nan(JOBS, "status", job_status.value, postgres.jobs_by_status)
    for priority in JobPriority:
        _set_labeled_or_nan(JOBS_BY_PRIORITY, "priority", priority.value, postgres.jobs_by_priority)
    _set_or_nan(
        JOB_ATTEMPTS_TOTAL,
        None if postgres.attempts_total is None else float(postgres.attempts_total),
    )
    for attempt_status in AttemptStatus:
        _set_labeled_or_nan(
            JOB_ATTEMPTS, "status", attempt_status.value, postgres.attempts_by_status
        )
    _set_or_nan(
        OUTBOX_UNPUBLISHED,
        None if postgres.outbox_unpublished is None else float(postgres.outbox_unpublished),
    )
    _set_or_nan(OUTBOX_OLDEST_AGE, postgres.outbox_oldest_unpublished_age_seconds)
    for event_type in (*KNOWN_OUTBOX_TYPES, UNKNOWN_OUTBOX_TYPE):
        _set_labeled_or_nan(
            OUTBOX_UNPUBLISHED_BY_TYPE,
            "event_type",
            event_type,
            postgres.outbox_unpublished_by_type,
        )
    _set_or_nan(OLDEST_QUEUED_AGE, postgres.oldest_queued_age_seconds)
    _set_or_nan(OLDEST_RUNNING_AGE, postgres.oldest_running_age_seconds)
    _set_or_nan(
        WORKERS_TOTAL,
        None if postgres.workers_total is None else float(postgres.workers_total),
    )

    redis_values = redis.streams if redis.available else None
    for label in STREAM_LABELS:
        if redis_values is None:
            REDIS_STREAM_LENGTH.labels(stream=label).set(_NAN)
            REDIS_PENDING.labels(stream=label).set(_NAN)
        else:
            counts = redis_values[label]
            REDIS_STREAM_LENGTH.labels(stream=label).set(float(counts.length))
            REDIS_PENDING.labels(stream=label).set(float(counts.pending))
    _set_or_nan(DELAYED_JOBS, None if redis.delayed_jobs is None else float(redis.delayed_jobs))
    _set_or_nan(DELAYED_DUE, None if redis.delayed_due is None else float(redis.delayed_due))
    _set_or_nan(
        DEAD_LETTER_LENGTH,
        None if redis.dead_letter_length is None else float(redis.dead_letter_length),
    )

    worker_counts = snapshot.workers.by_status if snapshot.workers is not None else None
    for worker_status in WorkerLivenessStatus:
        _set_labeled_or_nan(WORKERS, "status", worker_status.value, worker_counts)


async def render_latest() -> bytes:
    async with _LOCK:
        snapshot = await collect_system_snapshot()
        apply_snapshot(snapshot)
        return generate_latest(REGISTRY)
