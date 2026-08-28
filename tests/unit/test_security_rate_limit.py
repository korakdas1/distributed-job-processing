from __future__ import annotations

from job_platform.security.rate_limit import LimitClass, TokenBucketLimiter


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def time(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def test_write_burst_then_reject_then_refill() -> None:
    clock = FakeClock()
    limiter = TokenBucketLimiter(
        read_per_minute=600,
        read_burst=120,
        write_per_minute=60,
        write_burst=2,
        clock=clock.time,
    )
    allowed, _retry = limiter.consume("operator", LimitClass.WRITE)
    assert allowed is True
    allowed, _retry = limiter.consume("operator", LimitClass.WRITE)
    assert allowed is True
    allowed, retry = limiter.consume("operator", LimitClass.WRITE)
    assert allowed is False
    assert retry >= 1
    clock.advance(2.0)
    allowed, _retry = limiter.consume("operator", LimitClass.WRITE)
    assert allowed is True


def test_viewer_and_operator_do_not_share_buckets() -> None:
    limiter = TokenBucketLimiter(
        read_per_minute=60,
        read_burst=1,
        write_per_minute=60,
        write_burst=1,
    )
    assert limiter.consume("viewer", LimitClass.READ)[0] is True
    assert limiter.consume("viewer", LimitClass.READ)[0] is False
    assert limiter.consume("operator", LimitClass.READ)[0] is True
    assert limiter.bucket_count() == 2


def test_anonymous_is_not_bucketed() -> None:
    limiter = TokenBucketLimiter(
        read_per_minute=60,
        read_burst=1,
        write_per_minute=60,
        write_burst=1,
    )
    assert limiter.consume("anonymous", LimitClass.WRITE)[0] is True
    assert limiter.consume("anonymous", LimitClass.WRITE)[0] is True
    assert limiter.bucket_count() == 0
