"""In-process token-bucket limiter. Not Redis-backed and not globally distributed."""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

Clock = Callable[[], float]


class LimitClass(StrEnum):
    READ = "read"
    WRITE = "write"


@dataclass
class _Bucket:
    tokens: float
    last: float


class TokenBucketLimiter:
    """Principal-scoped token buckets keyed by stable names such as `viewer`."""

    def __init__(
        self,
        *,
        read_per_minute: int,
        read_burst: int,
        write_per_minute: int,
        write_burst: int,
        clock: Clock | None = None,
    ) -> None:
        self._read_rate = read_per_minute / 60.0
        self._read_burst = float(read_burst)
        self._write_rate = write_per_minute / 60.0
        self._write_burst = float(write_burst)
        self._clock: Clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._buckets: dict[tuple[str, LimitClass], _Bucket] = {}

    def consume(self, principal_name: str, limit_class: LimitClass) -> tuple[bool, int]:
        """Return (allowed, retry_after_seconds)."""
        if principal_name == "anonymous":
            return True, 0
        rate, burst = self._params(limit_class)
        key = (principal_name, limit_class)
        with self._lock:
            now = self._clock()
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _Bucket(tokens=burst, last=now)
                self._buckets[key] = bucket
            elapsed = max(0.0, now - bucket.last)
            bucket.tokens = min(burst, bucket.tokens + elapsed * rate)
            bucket.last = now
            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                return True, 0
            missing = 1.0 - bucket.tokens
            retry_after = missing / rate if rate > 0 else 60.0
            return False, max(1, int(math.ceil(retry_after)))

    def bucket_count(self) -> int:
        with self._lock:
            return len(self._buckets)

    def _params(self, limit_class: LimitClass) -> tuple[float, float]:
        if limit_class is LimitClass.READ:
            return self._read_rate, self._read_burst
        return self._write_rate, self._write_burst
