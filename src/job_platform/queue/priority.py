"""Canonical priority-to-stream mapping and weighted fair selection.

PostgreSQL Job.priority is authoritative. Redis stream names are scheduling
metadata. Workers must not infer the delivery stream from stored priority.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from job_platform.core.config import Settings, get_settings
from job_platform.core.enums import JobPriority

DEFAULT_WEIGHTS: dict[JobPriority, int] = {
    JobPriority.CRITICAL: 8,
    JobPriority.HIGH: 4,
    JobPriority.NORMAL: 2,
    JobPriority.LOW: 1,
}


class UnknownPriorityError(RuntimeError):
    """Job.priority is not a known routing key. Do not fall back to NORMAL."""


def ready_streams(settings: Settings | None = None) -> tuple[str, str, str, str]:
    cfg = settings if settings is not None else get_settings()
    return (
        cfg.redis_stream_critical,
        cfg.redis_stream_high,
        cfg.redis_stream_normal,
        cfg.redis_stream_low,
    )


def stream_for_priority(priority: str | JobPriority, settings: Settings | None = None) -> str:
    cfg = settings if settings is not None else get_settings()
    value = priority.value if isinstance(priority, JobPriority) else str(priority)
    mapping: dict[str, str] = {
        JobPriority.CRITICAL.value: cfg.redis_stream_critical,
        JobPriority.HIGH.value: cfg.redis_stream_high,
        JobPriority.NORMAL.value: cfg.redis_stream_normal,
        JobPriority.LOW.value: cfg.redis_stream_low,
    }
    stream = mapping.get(value)
    if stream is None:
        raise UnknownPriorityError(f"Unknown job priority {value!r}; refusing to route")
    return stream


def weights_for_streams(settings: Settings | None = None) -> dict[str, int]:
    cfg = settings if settings is not None else get_settings()
    return {
        cfg.redis_stream_critical: cfg.priority_weight_critical,
        cfg.redis_stream_high: cfg.priority_weight_high,
        cfg.redis_stream_normal: cfg.priority_weight_normal,
        cfg.redis_stream_low: cfg.priority_weight_low,
    }


class WeightedPrioritySelector:
    """Weighted quota cycle across ready streams.

    Default credits are 8/4/2/1 for critical/high/normal/low. A consumed
    message decrements that stream's credit. An empty stream is skipped without
    burning credit. When every stream's credit is exhausted, weights reset.

    This is weighted fairness, not strict priority: LOW still receives a slot
    while higher-priority streams remain backlogged.
    """

    def __init__(self, streams: Sequence[str], weights: Mapping[str, int]) -> None:
        if len(streams) == 0:
            raise ValueError("WeightedPrioritySelector requires at least one stream")
        self._streams = tuple(streams)
        self._weights = {stream: int(weights[stream]) for stream in self._streams}
        if any(value < 1 for value in self._weights.values()):
            raise ValueError("priority weights must be >= 1")
        self._credits = dict(self._weights)
        self._index = 0

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> WeightedPrioritySelector:
        cfg = settings if settings is not None else get_settings()
        return cls(ready_streams(cfg), weights_for_streams(cfg))

    def reset(self) -> None:
        self._credits = dict(self._weights)

    def streams_with_credit(self) -> frozenset[str]:
        return frozenset(stream for stream, credit in self._credits.items() if credit > 0)

    def has_zero_credit(self) -> bool:
        return any(credit <= 0 for credit in self._credits.values())

    def next_stream(self) -> str:
        """Return the next stream that still has credit, wrapping and resetting."""
        n = len(self._streams)
        if not self.streams_with_credit():
            self.reset()
        for _ in range(n):
            idx = self._index
            self._index = (self._index + 1) % n
            stream = self._streams[idx]
            if self._credits[stream] > 0:
                return stream
        self.reset()
        idx = self._index
        self._index = (self._index + 1) % n
        return self._streams[idx]

    def record_consumed(self, stream: str) -> None:
        if self._credits.get(stream, 0) > 0:
            self._credits[stream] -= 1
        if not self.streams_with_credit():
            self.reset()

    def skip_empty(self, stream: str) -> None:
        """Empty streams keep their remaining credit so later work is not delayed."""
        return None

    def select_available(self, available: set[str]) -> str | None:
        """Pure policy helper: pick among streams known to have work."""
        if not available:
            return None
        n = len(self._streams)
        for _ in range(n * 2):
            credited = self.streams_with_credit()
            if credited.isdisjoint(available):
                if self.has_zero_credit():
                    self.reset()
                    continue
                return None
            stream = self.next_stream()
            if stream in available:
                self.record_consumed(stream)
                return stream
            self.skip_empty(stream)
        return None
