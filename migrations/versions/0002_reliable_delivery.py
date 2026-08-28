"""Transactional outbox and attempt delivery tracking.

Revision ID: 0002_reliable_delivery
Revises: 0001_initial_jobs
Create Date: 2026-08-26

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_reliable_delivery"
down_revision: str | None = "0001_initial_jobs"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publish_attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("redis_message_id", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.CheckConstraint("publish_attempts >= 0", name="ck_outbox_events_publish_attempts"),
    )
    op.create_index("ix_outbox_events_job_id", "outbox_events", ["job_id"])
    op.create_index(
        "ix_outbox_events_unpublished_created_at",
        "outbox_events",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.add_column(
        "job_attempts",
        sa.Column("delivery_message_id", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("job_attempts", "delivery_message_id")
    op.drop_index("ix_outbox_events_unpublished_created_at", table_name="outbox_events")
    op.drop_index("ix_outbox_events_job_id", table_name="outbox_events")
    op.drop_table("outbox_events")
