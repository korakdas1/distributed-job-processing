"""Worker registry: durable process history. Liveness stays in Redis TTL.

Revision ID: 0005_worker_registry
Revises: 0004_priority_scheduling
Create Date: 2026-08-26

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_worker_registry"
down_revision: str | None = "0004_priority_scheduling"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workers",
        sa.Column("id", sa.Text(), primary_key=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("hostname", sa.Text(), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.CheckConstraint("pid > 0", name="ck_workers_pid_positive"),
    )
    op.create_index("ix_workers_started_at", "workers", ["started_at"])


def downgrade() -> None:
    op.drop_index("ix_workers_started_at", table_name="workers")
    op.drop_table("workers")
