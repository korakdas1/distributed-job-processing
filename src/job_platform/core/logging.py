"""Process logging helpers. Never log database passwords or connection URLs."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any


class _UtcFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=UTC)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat(timespec="seconds")


def configure_logging(level: str, *, service: str) -> None:
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            _UtcFormatter(
                fmt=(f"%(asctime)s %(levelname)s service={service} logger=%(name)s %(message)s"),
                datefmt="%Y-%m-%dT%H:%M:%SZ",
            )
        )
        root.addHandler(handler)
    root.setLevel(level.upper())


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    parts = [f"event={event}"]
    for key, value in fields.items():
        if value is not None:
            parts.append(f"{key}={value}")
    logger.log(level, " ".join(parts))
