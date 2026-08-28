from datetime import timedelta

from prometheus_client import generate_latest

from job_platform.api.app import create_app
from job_platform.core.clock import utcnow
from job_platform.core.enums import OperationalStatus
from job_platform.observability.metrics import REGISTRY
from job_platform.observability.middleware import UNMATCHED_ROUTE, route_template_from_scope
from job_platform.observability.snapshot import age_seconds, operational_status


def test_operational_status_matrix() -> None:
    assert operational_status(postgres=True, redis=True) is OperationalStatus.HEALTHY
    assert operational_status(postgres=True, redis=False) is OperationalStatus.DEGRADED
    assert operational_status(postgres=False, redis=True) is OperationalStatus.UNAVAILABLE
    assert operational_status(postgres=False, redis=False) is OperationalStatus.UNAVAILABLE


def test_age_seconds_clamps_negative() -> None:
    now = utcnow()
    assert age_seconds(now, None) == 0.0
    assert age_seconds(now, now + timedelta(seconds=5)) == 0.0
    assert age_seconds(now, now - timedelta(seconds=5)) == 5.0


def test_unmatched_route_template() -> None:
    assert route_template_from_scope({"type": "http"}) == UNMATCHED_ROUTE
    assert route_template_from_scope({"type": "http", "route": object()}) == UNMATCHED_ROUTE


def test_create_app_twice_does_not_duplicate_metrics() -> None:
    create_app()
    create_app()
    generate_latest(REGISTRY)
