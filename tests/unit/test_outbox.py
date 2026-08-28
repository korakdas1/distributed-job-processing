import uuid

from job_platform.api.services import create_job_record
from job_platform.core.config import Settings
from job_platform.core.enums import JobPriority
from job_platform.models.outbox import OUTBOX_EVENT_JOB_DISPATCH
from job_platform.outbox.events import create_dispatch_outbox_event


def _settings() -> Settings:
    return Settings(
        postgres_password="unused",
        retry_max_attempts=5,
        job_lease_timeout_seconds=60,
        payload_max_bytes=32 * 1024,
    )


def test_outbox_event_created_unpublished() -> None:
    job = create_job_record(
        settings=_settings(),
        job_type="word_count",
        payload={"text": "hello"},
        priority=JobPriority.NORMAL,
        max_attempts=None,
        timeout_seconds=None,
        delay_seconds=0,
    )
    event = create_dispatch_outbox_event(job)
    assert event.job_id == job.id
    assert event.event_type == OUTBOX_EVENT_JOB_DISPATCH
    assert event.published_at is None
    assert event.redis_message_id is None
    assert event.publish_attempts == 0
    assert event.last_error is None
    assert event.payload == {"priority": "NORMAL"}
    assert isinstance(event.id, uuid.UUID)
