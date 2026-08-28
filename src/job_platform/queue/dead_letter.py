"""Operational dead-letter stream jobs:dead. Not consumed by workers."""

from __future__ import annotations

import logging
import uuid
from typing import Any, cast

from job_platform.core.config import get_settings
from job_platform.core.logging import log_event
from job_platform.queue.client import get_redis

logger = logging.getLogger(__name__)


async def publish_dead_letter(
    job_id: uuid.UUID,
    *,
    outbox_event_id: uuid.UUID,
    reason: str,
    attempt_count: int,
    error_code: str | None = None,
) -> str:
    settings = get_settings()
    fields: dict[str, str] = {
        "job_id": str(job_id),
        "outbox_event_id": str(outbox_event_id),
        "reason": reason,
        "attempt_count": str(attempt_count),
    }
    if error_code is not None:
        fields["error_code"] = error_code
    message_id = await get_redis().xadd(
        settings.redis_dead_letter_stream,
        cast(Any, fields),
    )
    log_event(
        logger,
        "dead_letter_published",
        job_id=job_id,
        event_id=outbox_event_id,
        reason=reason,
        attempt_count=attempt_count,
        message_id=message_id,
    )
    return str(message_id)
