"""Startup security context: hashed keys, limiter, and effective flags."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass

from job_platform.core.config import Settings
from job_platform.security.rate_limit import TokenBucketLimiter

Clock = Callable[[], float]
_DUMMY_DIGEST = hashlib.sha256(b"job-platform-disabled-key-slot").digest()


def hash_api_key(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()


@dataclass(frozen=True, slots=True)
class SecurityContext:
    auth_enabled: bool
    rate_limit_enabled: bool
    docs_enabled: bool
    security_mode: str
    viewer_digest: bytes
    operator_digest: bytes
    limiter: TokenBucketLimiter
    allowed_hosts: tuple[str, ...]

    @classmethod
    def from_settings(cls, settings: Settings, *, clock: Clock | None = None) -> SecurityContext:
        viewer = settings.resolved_viewer_api_key() if settings.effective_auth_enabled else None
        operator = settings.resolved_operator_api_key() if settings.effective_auth_enabled else None
        return cls(
            auth_enabled=settings.effective_auth_enabled,
            rate_limit_enabled=settings.effective_rate_limit_enabled,
            docs_enabled=settings.effective_docs_enabled,
            security_mode=settings.security_mode,
            viewer_digest=hash_api_key(viewer) if viewer is not None else _DUMMY_DIGEST,
            operator_digest=hash_api_key(operator) if operator is not None else _DUMMY_DIGEST,
            limiter=TokenBucketLimiter(
                read_per_minute=settings.rate_limit_read_per_minute,
                read_burst=settings.rate_limit_read_burst,
                write_per_minute=settings.rate_limit_write_per_minute,
                write_burst=settings.rate_limit_write_burst,
                clock=clock,
            ),
            allowed_hosts=tuple(settings.allowed_host_list),
        )
