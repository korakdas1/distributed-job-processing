from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from job_platform.core.config import Settings
from job_platform.core.enums import WorkerLivenessStatus
from job_platform.worker.heartbeat import (
    heartbeat_key,
    heartbeat_view_from_redis,
    parse_heartbeat_payload,
)
from job_platform.worker.liveness import HeartbeatView, derive_worker_liveness


def test_heartbeat_key_format() -> None:
    assert heartbeat_key("worker-a8123f90") == "worker:worker-a8123f90:heartbeat"


def test_derive_stopped_wins_over_heartbeat() -> None:
    status, is_alive = derive_worker_liveness(
        stopped_at=datetime.now(UTC),
        redis_available=True,
        heartbeat=HeartbeatView(present=True, heartbeat_at=datetime.now(UTC), heartbeat_ttl_ms=500),
    )
    assert status is WorkerLivenessStatus.STOPPED
    assert is_alive is False


def test_derive_active() -> None:
    status, is_alive = derive_worker_liveness(
        stopped_at=None,
        redis_available=True,
        heartbeat=HeartbeatView(present=True, heartbeat_at=datetime.now(UTC), heartbeat_ttl_ms=400),
    )
    assert status is WorkerLivenessStatus.ACTIVE
    assert is_alive is True


def test_derive_expired() -> None:
    status, is_alive = derive_worker_liveness(
        stopped_at=None,
        redis_available=True,
        heartbeat=HeartbeatView(present=False),
    )
    assert status is WorkerLivenessStatus.EXPIRED
    assert is_alive is False


def test_derive_unknown_when_redis_unavailable() -> None:
    status, is_alive = derive_worker_liveness(
        stopped_at=None,
        redis_available=False,
        heartbeat=None,
    )
    assert status is WorkerLivenessStatus.UNKNOWN
    assert is_alive is None


def test_derive_stopped_when_redis_unavailable() -> None:
    status, is_alive = derive_worker_liveness(
        stopped_at=datetime.now(UTC),
        redis_available=False,
        heartbeat=None,
    )
    assert status is WorkerLivenessStatus.STOPPED
    assert is_alive is False


def test_malformed_heartbeat_is_unknown() -> None:
    status, is_alive = derive_worker_liveness(
        stopped_at=None,
        redis_available=True,
        heartbeat=HeartbeatView(present=False, malformed=True),
    )
    assert status is WorkerLivenessStatus.UNKNOWN
    assert is_alive is None


def test_parse_heartbeat_rejects_garbage() -> None:
    assert parse_heartbeat_payload("garbage", worker_id="worker-aaaaaaa1") is None
    assert parse_heartbeat_payload("[]", worker_id="worker-aaaaaaa1") is None
    view = heartbeat_view_from_redis(worker_id="worker-aaaaaaa1", raw="not-json", pttl_ms=800)
    assert view.malformed is True
    assert view.present is False


def test_heartbeat_pttl_missing_means_expired() -> None:
    view = heartbeat_view_from_redis(
        worker_id="worker-aaaaaaa1",
        raw='{"worker_id":"worker-aaaaaaa1","heartbeat_at":"2026-08-26T00:00:00+00:00"}',
        pttl_ms=-2,
    )
    assert view.present is False
    assert view.malformed is False
    assert view.nonexpiring is False


def test_heartbeat_pttl_minus_one_is_not_active() -> None:
    view = heartbeat_view_from_redis(
        worker_id="worker-aaaaaaa1",
        raw='{"worker_id":"worker-aaaaaaa1","heartbeat_at":"2026-08-26T00:00:00+00:00"}',
        pttl_ms=-1,
    )
    assert view.nonexpiring is True
    assert view.present is False
    status, is_alive = derive_worker_liveness(
        stopped_at=None,
        redis_available=True,
        heartbeat=view,
    )
    assert status is WorkerLivenessStatus.UNKNOWN
    assert is_alive is None


def test_heartbeat_identity_mismatch_is_unknown() -> None:
    view = heartbeat_view_from_redis(
        worker_id="worker-aaaaaaa1",
        raw='{"worker_id":"worker-bbbbbbbb","heartbeat_at":"2026-08-26T00:00:00+00:00"}',
        pttl_ms=800,
    )
    assert view.identity_mismatch is True
    assert view.present is False
    status, is_alive = derive_worker_liveness(
        stopped_at=None,
        redis_available=True,
        heartbeat=view,
    )
    assert status is WorkerLivenessStatus.UNKNOWN
    assert is_alive is None


def test_naive_heartbeat_timestamp_is_malformed() -> None:
    view = heartbeat_view_from_redis(
        worker_id="worker-aaaaaaa1",
        raw='{"worker_id":"worker-aaaaaaa1","heartbeat_at":"2026-08-26T00:00:00"}',
        pttl_ms=800,
    )
    assert view.malformed is True
    assert (
        parse_heartbeat_payload(
            '{"worker_id":"worker-aaaaaaa1","heartbeat_at":"2026-08-26T00:00:00"}',
            worker_id="worker-aaaaaaa1",
        )
        is None
    )


def test_heartbeat_ttl_must_cover_two_intervals() -> None:
    with pytest.raises(ValidationError):
        Settings(
            worker_heartbeat_interval_seconds=2.0,
            worker_heartbeat_ttl_seconds=3.0,
            worker_db_heartbeat_interval_seconds=2.0,
        )


def test_db_sample_interval_must_not_be_faster_than_heartbeat() -> None:
    with pytest.raises(ValidationError):
        Settings(
            worker_heartbeat_interval_seconds=2.0,
            worker_heartbeat_ttl_seconds=6.0,
            worker_db_heartbeat_interval_seconds=1.0,
        )
