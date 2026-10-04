"""Pytest fixtures.

Integration tests use a dedicated local database named `job_platform_test`.
They never truncate or drop the development database `job_platform`.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from job_platform.core.config import get_settings
from tests.disposable_infrastructure import DisposableInfrastructure, load_infrastructure

os.environ.setdefault("WORKER_HEARTBEAT_INTERVAL_SECONDS", "0.2")
os.environ.setdefault("WORKER_HEARTBEAT_TTL_SECONDS", "1.0")
os.environ.setdefault("WORKER_DB_HEARTBEAT_INTERVAL_SECONDS", "0.4")
os.environ["SECURITY_MODE"] = "development"
os.environ["AUTH_ENABLED"] = "false"
os.environ["RATE_LIMIT_ENABLED"] = "false"
# Host/shell leftovers from Compose/acceptance must not leak into Settings().
for _name in (
    "COMPOSE_PROJECT_NAME",
    "VIEWER_API_KEY",
    "VIEWER_API_KEY_FILE",
    "OPERATOR_API_KEY",
    "OPERATOR_API_KEY_FILE",
    "TLS_CERT_FILE",
    "TLS_KEY_FILE",
):
    os.environ.pop(_name, None)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-disruptive", action="store_true", help="Opt into isolated service outages"
    )
    parser.addoption(
        "--disposable-manifest", type=Path, help="Infrastructure created by the safe runner"
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not config.getoption("--run-disruptive"):
        skip = pytest.mark.skip(reason="Service disruption requires python -m tests.run_disruptive")
        for item in items:
            if item.get_closest_marker("disruptive"):
                item.add_marker(skip)


def infrastructure_for(config: pytest.Config) -> DisposableInfrastructure:
    if not config.getoption("--run-disruptive"):
        raise pytest.UsageError(
            "Disruptive helpers require --run-disruptive and the isolated runner"
        )
    path = config.getoption("--disposable-manifest")
    if path is None:
        raise pytest.UsageError(
            "Use python -m tests.run_disruptive to create disposable infrastructure"
        )
    infrastructure = load_infrastructure(path, opted_in=True)
    get_settings.cache_clear()
    infrastructure.check_targets(get_settings())
    infrastructure.verify()
    return infrastructure


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    # Run before database/autouse fixtures, so unsafe opt-in cannot even truncate a store.
    if item.get_closest_marker("disruptive") and item.config.getoption("--run-disruptive"):
        infrastructure_for(item.config)


@pytest.fixture
def disposable_infrastructure(request: pytest.FixtureRequest) -> DisposableInfrastructure:
    if request.node.get_closest_marker("disruptive") is None:
        raise pytest.UsageError("Service-control tests must have the disruptive marker")
    return infrastructure_for(request.config)
