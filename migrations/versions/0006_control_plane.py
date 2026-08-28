"""Idempotency records and cooperative cancellation columns.

Revision ID: 0006_control_plane
Revises: 0005_worker_registry
Create Date: 2026-08-27

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006_control_plane"
down_revision: str | None = "0005_worker_registry"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_ATTEMPT_WITH_CANCELLED = (
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "INTERRUPTED",
    "CANCELLED",
)
_ATTEMPT_WITHOUT_CANCELLED = (
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "INTERRUPTED",
)


def _in_clause(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        sa.text(
            """
            UPDATE jobs
            SET
                cancel_requested_at = COALESCE(
                    cancel_requested_at, cancelled_at, completed_at, created_at
                ),
                cancelled_at = COALESCE(cancelled_at, completed_at, created_at),
                completed_at = COALESCE(completed_at, cancelled_at, created_at)
            WHERE status = 'CANCELLED'
            """
        )
    )
    op.create_check_constraint(
        "ck_jobs_cancelled_requires_request",
        "jobs",
        "cancelled_at IS NULL OR cancel_requested_at IS NOT NULL",
    )
    op.create_check_constraint(
        "ck_jobs_cancelled_requires_cancelled_at",
        "jobs",
        "status <> 'CANCELLED' OR cancelled_at IS NOT NULL",
    )
    op.create_check_constraint(
        "ck_jobs_cancelled_requires_completed_at",
        "jobs",
        "status <> 'CANCELLED' OR completed_at IS NOT NULL",
    )

    op.drop_constraint("ck_job_attempts_status", "job_attempts", type_="check")
    op.create_check_constraint(
        "ck_job_attempts_status",
        "job_attempts",
        f"status IN ({_in_clause(_ATTEMPT_WITH_CANCELLED)})",
    )

    op.create_table(
        "submission_idempotency",
        sa.Column("key_hash", sa.Text(), nullable=False),
        sa.Column("request_fingerprint", sa.Text(), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("key_hash", name="pk_submission_idempotency"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.CheckConstraint("char_length(key_hash) = 64", name="ck_submission_idempotency_hash"),
        sa.CheckConstraint(
            "char_length(request_fingerprint) = 64",
            name="ck_submission_idempotency_fingerprint",
        ),
    )
    op.create_index(
        "ix_submission_idempotency_job_id",
        "submission_idempotency",
        ["job_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_submission_idempotency_job_id", table_name="submission_idempotency")
    op.drop_table("submission_idempotency")
    op.drop_constraint("ck_job_attempts_status", "job_attempts", type_="check")
    op.create_check_constraint(
        "ck_job_attempts_status",
        "job_attempts",
        f"status IN ({_in_clause(_ATTEMPT_WITHOUT_CANCELLED)})",
    )
    op.drop_constraint("ck_jobs_cancelled_requires_completed_at", "jobs", type_="check")
    op.drop_constraint("ck_jobs_cancelled_requires_cancelled_at", "jobs", type_="check")
    op.drop_constraint("ck_jobs_cancelled_requires_request", "jobs", type_="check")
    op.drop_column("jobs", "cancel_requested_at")
