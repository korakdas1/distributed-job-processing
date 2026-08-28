"""Retry delivery: outbox payload, RETRYING invariant, retry index.

Revision ID: 0003_retry_delivery
Revises: 0002_reliable_delivery
Create Date: 2026-08-26

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003_retry_delivery"
down_revision: str | None = "0002_reliable_delivery"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "outbox_events",
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.create_check_constraint(
        "ck_jobs_retrying_requires_next_retry_at",
        "jobs",
        "status <> 'RETRYING' OR next_retry_at IS NOT NULL",
    )
    op.create_index(
        "ix_jobs_retrying_next_retry_at",
        "jobs",
        ["next_retry_at"],
        postgresql_where=sa.text("status = 'RETRYING'"),
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_retrying_next_retry_at", table_name="jobs")
    op.drop_constraint("ck_jobs_retrying_requires_next_retry_at", "jobs", type_="check")
    op.drop_column("outbox_events", "payload")
