"""Durable hashed Idempotency-Key records. Raw keys are never stored."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from job_platform.db.base import Base


class SubmissionIdempotency(Base):
    __tablename__ = "submission_idempotency"
    __table_args__ = (
        CheckConstraint("char_length(key_hash) = 64", name="ck_submission_idempotency_hash"),
        CheckConstraint(
            "char_length(request_fingerprint) = 64",
            name="ck_submission_idempotency_fingerprint",
        ),
        Index("ix_submission_idempotency_job_id", "job_id"),
    )

    key_hash: Mapped[str] = mapped_column(Text, primary_key=True)
    request_fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
