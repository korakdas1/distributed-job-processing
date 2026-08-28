"""Durable worker process history. Liveness is derived from Redis TTL, not this table."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from job_platform.db.base import Base


class Worker(Base):
    __tablename__ = "workers"
    __table_args__ = (
        CheckConstraint("pid > 0", name="ck_workers_pid_positive"),
        Index("ix_workers_started_at", "started_at"),
    )

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    hostname: Mapped[str] = mapped_column(Text, nullable=False)
    pid: Mapped[int] = mapped_column(Integer, nullable=False)
