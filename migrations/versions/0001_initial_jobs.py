"""Create jobs and job_attempts tables.

Revision ID: 0001_initial_jobs
Revises:
Create Date: 2026-08-25

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial_jobs"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

JOB_STATUSES = (
    "SCHEDULED",
    "QUEUED",
    "RUNNING",
    "RETRYING",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
)
JOB_PRIORITIES = ("CRITICAL", "HIGH", "NORMAL", "LOW")
JOB_TYPES = (
    "word_count",
    "sum_numbers",
    "sleep",
    "prime_calculation",
    "simulate_failure",
)
ATTEMPT_STATUSES = ("RUNNING", "SUCCEEDED", "FAILED", "INTERRUPTED")


def _in_clause(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("job_type", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("priority", sa.Text(), nullable=False),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("run_after", sa.DateTime(timezone=True), nullable=True),
        sa.Column("worker_id", sa.Text(), nullable=True),
        sa.Column("idempotency_key", sa.Text(), nullable=True),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False),
        sa.CheckConstraint(f"status IN ({_in_clause(JOB_STATUSES)})", name="ck_jobs_status"),
        sa.CheckConstraint(f"priority IN ({_in_clause(JOB_PRIORITIES)})", name="ck_jobs_priority"),
        sa.CheckConstraint(f"job_type IN ({_in_clause(JOB_TYPES)})", name="ck_jobs_job_type"),
        sa.CheckConstraint("attempt_count >= 0", name="ck_jobs_attempt_count_non_negative"),
        sa.CheckConstraint("max_attempts >= 1", name="ck_jobs_max_attempts_positive"),
        sa.CheckConstraint("timeout_seconds > 0", name="ck_jobs_timeout_positive"),
    )
    op.create_index("ix_jobs_status", "jobs", ["status"])
    op.create_index("ix_jobs_job_type", "jobs", ["job_type"])
    op.create_index("ix_jobs_priority", "jobs", ["priority"])
    op.create_index("ix_jobs_created_at", "jobs", ["created_at"])

    op.create_table(
        "job_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.Text(), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'RUNNING'"),
        ),
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("job_id", "attempt_number", name="uq_job_attempts_job_number"),
        sa.CheckConstraint("attempt_number >= 1", name="ck_job_attempts_number_positive"),
        sa.CheckConstraint(
            f"status IN ({_in_clause(ATTEMPT_STATUSES)})",
            name="ck_job_attempts_status",
        ),
        sa.CheckConstraint(
            "duration_ms IS NULL OR duration_ms >= 0",
            name="ck_job_attempts_duration_non_negative",
        ),
    )


def downgrade() -> None:
    op.drop_table("job_attempts")
    op.drop_index("ix_jobs_created_at", table_name="jobs")
    op.drop_index("ix_jobs_priority", table_name="jobs")
    op.drop_index("ix_jobs_job_type", table_name="jobs")
    op.drop_index("ix_jobs_status", table_name="jobs")
    op.drop_table("jobs")
