from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text

from job_platform.core.config import get_settings

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]


def test_alembic_history_and_schema(test_database: None) -> None:
    cfg = Config(str(ROOT / "alembic.ini"))
    script = ScriptDirectory.from_config(cfg)
    revisions = list(script.walk_revisions())
    assert [item.revision for item in revisions] == [
        "0006_control_plane",
        "0005_worker_registry",
        "0004_priority_scheduling",
        "0003_retry_delivery",
        "0002_reliable_delivery",
        "0001_initial_jobs",
    ]

    command.downgrade(cfg, "0003_retry_delivery")
    settings = get_settings()
    assert settings.postgres_db == "job_platform_test"
    engine = create_engine(settings.sqlalchemy_url)
    try:
        with engine.begin() as connection:
            job_id = connection.execute(
                text(
                    """
                    INSERT INTO jobs (
                        id, job_type, payload, status, priority, attempt_count,
                        max_attempts, timeout_seconds
                    ) VALUES (
                        gen_random_uuid(), 'word_count', '{"text":"legacy"}'::jsonb,
                        'QUEUED', 'NORMAL', 1, 5, 60
                    )
                    RETURNING id
                    """
                )
            ).scalar_one()
            connection.execute(
                text(
                    """
                    INSERT INTO job_attempts (
                        id, job_id, attempt_number, worker_id, status, delivery_message_id
                    ) VALUES (
                        gen_random_uuid(), :job_id, 1, 'worker-legacy', 'SUCCEEDED', '42-0'
                    )
                    """
                ),
                {"job_id": job_id},
            )
            null_attempt_job = connection.execute(
                text(
                    """
                    INSERT INTO jobs (
                        id, job_type, payload, status, priority, attempt_count,
                        max_attempts, timeout_seconds
                    ) VALUES (
                        gen_random_uuid(), 'word_count', '{"text":"untracked"}'::jsonb,
                        'QUEUED', 'NORMAL', 1, 5, 60
                    )
                    RETURNING id
                    """
                )
            ).scalar_one()
            connection.execute(
                text(
                    """
                    INSERT INTO job_attempts (
                        id, job_id, attempt_number, worker_id, status
                    ) VALUES (
                        gen_random_uuid(), :job_id, 1, 'worker-legacy', 'SUCCEEDED'
                    )
                    """
                ),
                {"job_id": null_attempt_job},
            )
    finally:
        engine.dispose()

    command.upgrade(cfg, "head")
    command.downgrade(cfg, "0003_retry_delivery")
    command.upgrade(cfg, "head")

    engine = create_engine(settings.sqlalchemy_url)
    try:
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        assert "jobs" in tables
        assert "job_attempts" in tables
        assert "outbox_events" in tables
        assert "workers" in tables
        assert "submission_idempotency" in tables

        job_columns = {col["name"] for col in inspector.get_columns("jobs")}
        expected = {
            "id",
            "job_type",
            "payload",
            "status",
            "priority",
            "result",
            "error",
            "attempt_count",
            "max_attempts",
            "created_at",
            "queued_at",
            "started_at",
            "completed_at",
            "cancelled_at",
            "cancel_requested_at",
            "next_retry_at",
            "run_after",
            "worker_id",
            "idempotency_key",
            "timeout_seconds",
        }
        assert expected <= job_columns

        attempt_columns = {col["name"] for col in inspector.get_columns("job_attempts")}
        assert "delivery_message_id" in attempt_columns
        assert "delivery_stream" in attempt_columns

        outbox_columns = {col["name"] for col in inspector.get_columns("outbox_events")}
        assert {
            "id",
            "job_id",
            "event_type",
            "payload",
            "created_at",
            "published_at",
            "publish_attempts",
            "last_error",
            "redis_message_id",
        } <= outbox_columns
        payload_col = next(
            col for col in inspector.get_columns("outbox_events") if col["name"] == "payload"
        )
        assert str(payload_col["type"]).upper().startswith("JSONB")

        indexes = {idx["name"] for idx in inspector.get_indexes("jobs")}
        assert {
            "ix_jobs_status",
            "ix_jobs_job_type",
            "ix_jobs_priority",
            "ix_jobs_created_at",
            "ix_jobs_retrying_next_retry_at",
            "ix_jobs_scheduled_run_after",
        } <= indexes
        outbox_indexes = {idx["name"] for idx in inspector.get_indexes("outbox_events")}
        assert "ix_outbox_events_unpublished_created_at" in outbox_indexes
        assert "ix_outbox_events_job_id" in outbox_indexes

        fks = inspector.get_foreign_keys("outbox_events")
        assert any(fk["referred_table"] == "jobs" for fk in fks)
        attempt_fks = inspector.get_foreign_keys("job_attempts")
        assert all(fk["referred_table"] != "workers" for fk in attempt_fks)
        job_fks = inspector.get_foreign_keys("jobs")
        assert all(fk["referred_table"] != "workers" for fk in job_fks)

        worker_columns = {col["name"] for col in inspector.get_columns("workers")}
        assert {
            "id",
            "started_at",
            "last_seen_at",
            "stopped_at",
            "hostname",
            "pid",
        } <= worker_columns
        worker_indexes = {idx["name"] for idx in inspector.get_indexes("workers")}
        assert "ix_workers_started_at" in worker_indexes
        pk = inspector.get_pk_constraint("workers")
        assert pk["constrained_columns"] == ["id"]

        with engine.connect() as connection:
            current = connection.execute(text("SELECT version_num FROM alembic_version"))
            assert current.scalar_one() == "0006_control_plane"
            constraints = {
                row[0]
                for row in connection.execute(
                    text(
                        """
                        SELECT conname FROM pg_constraint
                        WHERE conname IN (
                            'ck_jobs_retrying_requires_next_retry_at',
                            'ck_jobs_scheduled_requires_run_after',
                            'ck_job_attempts_delivery_identity',
                            'ck_workers_pid_positive',
                            'ck_jobs_cancelled_requires_request',
                            'ck_jobs_cancelled_requires_cancelled_at',
                            'ck_jobs_cancelled_requires_completed_at',
                            'pk_submission_idempotency'
                        )
                        """
                    )
                )
            }
            assert "ck_jobs_retrying_requires_next_retry_at" in constraints
            assert "ck_jobs_scheduled_requires_run_after" in constraints
            assert "ck_job_attempts_delivery_identity" in constraints
            assert "ck_workers_pid_positive" in constraints
            assert "ck_jobs_cancelled_requires_request" in constraints
            assert "ck_jobs_cancelled_requires_cancelled_at" in constraints
            assert "ck_jobs_cancelled_requires_completed_at" in constraints
            assert "pk_submission_idempotency" in constraints
            allowed_attempts = connection.execute(
                text(
                    """
                    SELECT pg_get_constraintdef(oid) FROM pg_constraint
                    WHERE conname = 'ck_job_attempts_status'
                    """
                )
            ).scalar_one()
            assert "CANCELLED" in allowed_attempts
            tracked = connection.execute(
                text(
                    """
                    SELECT delivery_stream FROM job_attempts
                    WHERE delivery_message_id = '42-0'
                    """
                )
            ).scalar_one()
            assert tracked == "jobs:normal"
            untracked = connection.execute(
                text(
                    """
                    SELECT delivery_stream FROM job_attempts
                    WHERE delivery_message_id IS NULL
                    LIMIT 1
                    """
                )
            ).scalar_one()
            assert untracked is None
            leftover_attempts = connection.execute(
                text("SELECT COUNT(*) FROM job_attempts")
            ).scalar_one()
            assert leftover_attempts >= 2
    finally:
        engine.dispose()

    command.downgrade(cfg, "0004_priority_scheduling")
    engine = create_engine(settings.sqlalchemy_url)
    try:
        inspector = inspect(engine)
        assert "workers" not in set(inspector.get_table_names())
        assert "job_attempts" in set(inspector.get_table_names())
    finally:
        engine.dispose()
    command.upgrade(cfg, "head")
    engine = create_engine(settings.sqlalchemy_url)
    try:
        inspector = inspect(engine)
        assert "workers" in set(inspector.get_table_names())
        with engine.connect() as connection:
            current = connection.execute(text("SELECT version_num FROM alembic_version"))
            assert current.scalar_one() == "0006_control_plane"
    finally:
        engine.dispose()

    command.downgrade(cfg, "0005_worker_registry")
    engine = create_engine(settings.sqlalchemy_url)
    try:
        inspector = inspect(engine)
        assert "submission_idempotency" not in set(inspector.get_table_names())
        job_columns = {col["name"] for col in inspector.get_columns("jobs")}
        assert "cancel_requested_at" not in job_columns
        with engine.connect() as connection:
            current = connection.execute(text("SELECT version_num FROM alembic_version"))
            assert current.scalar_one() == "0005_worker_registry"
    finally:
        engine.dispose()
    command.upgrade(cfg, "head")
    engine = create_engine(settings.sqlalchemy_url)
    try:
        inspector = inspect(engine)
        assert "submission_idempotency" in set(inspector.get_table_names())
        with engine.connect() as connection:
            current = connection.execute(text("SELECT version_num FROM alembic_version"))
            assert current.scalar_one() == "0006_control_plane"
    finally:
        engine.dispose()
