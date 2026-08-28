"""Durable job and attempt models. Schema is applied only via Alembic."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from job_platform.core.enums import AttemptStatus, JobPriority, JobStatus, JobType
from job_platform.db.base import Base

_STATUS_VALUES = ", ".join(f"'{item.value}'" for item in JobStatus)
_PRIORITY_VALUES = ", ".join(f"'{item.value}'" for item in JobPriority)
_ATTEMPT_STATUS_VALUES = ", ".join(f"'{item.value}'" for item in AttemptStatus)
_JOB_TYPE_VALUES = ", ".join(f"'{item.value}'" for item in JobType)


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(f"status IN ({_STATUS_VALUES})", name="ck_jobs_status"),
        CheckConstraint(f"priority IN ({_PRIORITY_VALUES})", name="ck_jobs_priority"),
        CheckConstraint(f"job_type IN ({_JOB_TYPE_VALUES})", name="ck_jobs_job_type"),
        CheckConstraint("attempt_count >= 0", name="ck_jobs_attempt_count_non_negative"),
        CheckConstraint("max_attempts >= 1", name="ck_jobs_max_attempts_positive"),
        CheckConstraint("timeout_seconds > 0", name="ck_jobs_timeout_positive"),
        CheckConstraint(
            "status <> 'RETRYING' OR next_retry_at IS NOT NULL",
            name="ck_jobs_retrying_requires_next_retry_at",
        ),
        CheckConstraint(
            "status <> 'SCHEDULED' OR run_after IS NOT NULL",
            name="ck_jobs_scheduled_requires_run_after",
        ),
        CheckConstraint(
            "cancelled_at IS NULL OR cancel_requested_at IS NOT NULL",
            name="ck_jobs_cancelled_requires_request",
        ),
        CheckConstraint(
            "status <> 'CANCELLED' OR cancelled_at IS NOT NULL",
            name="ck_jobs_cancelled_requires_cancelled_at",
        ),
        CheckConstraint(
            "status <> 'CANCELLED' OR completed_at IS NOT NULL",
            name="ck_jobs_cancelled_requires_completed_at",
        ),
        Index("ix_jobs_status", "status"),
        Index("ix_jobs_job_type", "job_type"),
        Index("ix_jobs_priority", "priority"),
        Index("ix_jobs_created_at", "created_at"),
        Index(
            "ix_jobs_retrying_next_retry_at",
            "next_retry_at",
            postgresql_where=text("status = 'RETRYING'"),
        ),
        Index(
            "ix_jobs_scheduled_run_after",
            "run_after",
            postgresql_where=text("status = 'SCHEDULED'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[str] = mapped_column(Text, nullable=False)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    run_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    worker_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False)

    attempts: Mapped[list[JobAttempt]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )


class JobAttempt(Base):
    __tablename__ = "job_attempts"
    __table_args__ = (
        UniqueConstraint("job_id", "attempt_number", name="uq_job_attempts_job_number"),
        CheckConstraint("attempt_number >= 1", name="ck_job_attempts_number_positive"),
        CheckConstraint(
            f"status IN ({_ATTEMPT_STATUS_VALUES})",
            name="ck_job_attempts_status",
        ),
        CheckConstraint(
            "duration_ms IS NULL OR duration_ms >= 0",
            name="ck_job_attempts_duration_non_negative",
        ),
        CheckConstraint(
            "(delivery_message_id IS NULL AND delivery_stream IS NULL) "
            "OR (delivery_message_id IS NOT NULL AND delivery_stream IS NOT NULL)",
            name="ck_job_attempts_delivery_identity",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'RUNNING'"))
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    delivery_message_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    delivery_stream: Mapped[str | None] = mapped_column(Text, nullable=True)

    job: Mapped[Job] = relationship(back_populates="attempts")
