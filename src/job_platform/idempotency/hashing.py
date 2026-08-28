"""Hash Idempotency-Key values and canonical request fingerprints."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from job_platform.core.config import Settings
from job_platform.core.errors import AppError
from job_platform.schemas.job import JobCreateRequest
from job_platform.tasks.definitions import parse_job_type, validate_payload

IDEMPOTENCY_KEY_MIN_LENGTH = 1
IDEMPOTENCY_KEY_MAX_LENGTH = 128


def hash_idempotency_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def validate_idempotency_key(key: str) -> str:
    if len(key) < IDEMPOTENCY_KEY_MIN_LENGTH or len(key) > IDEMPOTENCY_KEY_MAX_LENGTH:
        raise AppError(
            "INVALID_IDEMPOTENCY_KEY",
            "Idempotency-Key must be between 1 and 128 characters.",
            status_code=422,
        )
    if any(ord(char) < 32 or ord(char) == 127 for char in key):
        raise AppError(
            "INVALID_IDEMPOTENCY_KEY",
            "Idempotency-Key must not contain control characters.",
            status_code=422,
        )
    return key


def canonical_request_fingerprint(body: JobCreateRequest, settings: Settings) -> str:
    parsed_type = parse_job_type(body.job_type)
    payload = validate_payload(parsed_type, body.payload)
    document: dict[str, Any] = {
        "delay_seconds": body.delay_seconds,
        "job_type": parsed_type.value,
        "max_attempts": (
            body.max_attempts if body.max_attempts is not None else settings.retry_max_attempts
        ),
        "payload": payload,
        "priority": body.priority.value,
        "timeout_seconds": (
            body.timeout_seconds
            if body.timeout_seconds is not None
            else settings.job_lease_timeout_seconds
        ),
    }
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
