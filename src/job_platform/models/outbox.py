"""Transactional outbox events. Schema is applied only via Alembic."""

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
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from job_platform.db.base import Base
from job_platform.models.job import Job

OUTBOX_EVENT_JOB_DISPATCH = "JOB_DISPATCH"
OUTBOX_EVENT_JOB_RETRY_SCHEDULE = "JOB_RETRY_SCHEDULE"
OUTBOX_EVENT_JOB_INITIAL_SCHEDULE = "JOB_INITIAL_SCHEDULE"
OUTBOX_EVENT_JOB_DEAD_LETTER = "JOB_DEAD_LETTER"


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint("publish_attempts >= 0", name="ck_outbox_events_publish_attempts"),
        Index(
            "ix_outbox_events_unpublished_created_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
        Index("ix_outbox_events_job_id", "job_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    publish_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    redis_message_id: Mapped[str | None] = mapped_column(Text, nullable=True)

    job: Mapped[Job] = relationship()
