"""Unhandled 500 envelope, request ID, and HTTP metrics."""

from __future__ import annotations

import logging
import uuid
from typing import Any

import pytest
from httpx import AsyncClient

from job_platform.core.logging import log_event
from tests.integration.test_metrics import sample_float

pytestmark = pytest.mark.integration


async def test_unhandled_500_is_safe(client: AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    async def boom(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("secret internal detail")

    events: list[tuple[str, dict[str, Any]]] = []
    original = log_event

    def capture(
        logger: logging.Logger, event: str, *, level: int = logging.INFO, **fields: Any
    ) -> None:
        events.append((event, dict(fields)))
        original(logger, event, level=level, **fields)

    monkeypatch.setattr("job_platform.api.routes.jobs.get_job_by_id", boom)
    monkeypatch.setattr("job_platform.observability.middleware.log_event", capture)
    job_id = uuid.uuid4()
    response = await client.get(f"/jobs/{job_id}")
    assert response.status_code == 500
    body = response.json()
    assert body["error"]["code"] == "INTERNAL_SERVER_ERROR"
    assert body["error"]["message"] == "An unexpected internal error occurred."
    assert "secret internal detail" not in response.text
    assert "RuntimeError" not in response.text
    assert "Traceback" not in response.text
    request_id = response.headers["x-request-id"]
    assert request_id
    assert body["error"]["request_id"] == request_id
    completed = [fields for event, fields in events if event == "http_request_completed"]
    assert completed
    assert completed[0]["request_id"] == request_id
    assert completed[0]["status_code"] == 500
    assert completed[0]["route"] == "/jobs/{job_id}"
    metrics = await client.get("/metrics")
    assert (
        sample_float(
            metrics.text,
            "job_platform_http_requests_total",
            method="GET",
            route="/jobs/{job_id}",
            status_code="500",
        )
        >= 1
    )
    assert "job_platform_http_request_duration_seconds_bucket" in metrics.text
    print(f"UNHANDLED_500 request_id={request_id}")
