"""Pytest fixtures.

Integration tests use a dedicated local database named `job_platform_test`.
They never truncate or drop the development database `job_platform`.
"""

from __future__ import annotations

import os

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
