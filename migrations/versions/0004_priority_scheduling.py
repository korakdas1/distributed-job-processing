"""Priority scheduling: delivery stream identity and operational SCHEDULED.

Revision ID: 0004_priority_scheduling
Revises: 0003_retry_delivery
Create Date: 2026-08-26

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_priority_scheduling"
down_revision: str | None = "0003_retry_delivery"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("job_attempts", sa.Column("delivery_stream", sa.Text(), nullable=True))
    op.execute(
        """
        UPDATE job_attempts
        SET delivery_stream = 'jobs:normal'
        WHERE delivery_message_id IS NOT NULL
        """
    )
    op.create_check_constraint(
        "ck_job_attempts_delivery_identity",
        "job_attempts",
        "(delivery_message_id IS NULL AND delivery_stream IS NULL) "
        "OR (delivery_message_id IS NOT NULL AND delivery_stream IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_jobs_scheduled_requires_run_after",
        "jobs",
        "status <> 'SCHEDULED' OR run_after IS NOT NULL",
    )
    op.create_index(
        "ix_jobs_scheduled_run_after",
        "jobs",
        ["run_after"],
        postgresql_where=sa.text("status = 'SCHEDULED'"),
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_scheduled_run_after", table_name="jobs")
    op.drop_constraint("ck_jobs_scheduled_requires_run_after", "jobs", type_="check")
    op.drop_constraint("ck_job_attempts_delivery_identity", "job_attempts", type_="check")
    op.drop_column("job_attempts", "delivery_stream")
