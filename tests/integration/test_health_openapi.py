import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.integration


async def test_health_does_not_need_postgres(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "api"}


async def test_ready_when_postgres_up(client: AsyncClient) -> None:
    response = await client.get("/ready")
    assert response.status_code == 200
    assert response.json()["status"] == "ready"
    assert response.json()["degraded"] is False
    assert response.json()["dependencies"]["postgres"] == "up"
    assert response.json()["dependencies"]["redis"] == "up"
    assert response.json()["checks"]["postgres"] == "ok"


async def test_openapi_implemented_routes(client: AsyncClient) -> None:
    response = await client.get("/openapi.json")
    assert response.status_code == 200
    paths = set(response.json()["paths"])
    assert paths == {
        "/jobs",
        "/jobs/{job_id}",
        "/health",
        "/ready",
        "/workers",
        "/metrics",
        "/metrics/summary",
    }
    job_item = response.json()["paths"]["/jobs"]
    assert "post" in job_item
    assert "get" in job_item
    assert "delete" not in job_item
    post_params = job_item["post"].get("parameters") or []
    assert any(
        item.get("name") == "Idempotency-Key" and item.get("in") == "header" for item in post_params
    )
    job_by_id = response.json()["paths"]["/jobs/{job_id}"]
    assert "get" in job_by_id
    assert "delete" in job_by_id
    assert "get" in response.json()["paths"]["/workers"]
    assert "post" not in response.json()["paths"]["/workers"]
    assert "/metrics/jobs" not in paths
