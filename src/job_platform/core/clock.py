"""UTC clock helper shared by the API and worker."""

from datetime import UTC, datetime


def utcnow() -> datetime:
    return datetime.now(UTC)
