from __future__ import annotations

import asyncio
import uuid

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from job_platform.api.app import create_app
from job_platform.core.config import get_settings
from job_platform.db.session import dispose_engine

pytestmark = pytest.mark.integration

WORD_COUNT = {
    "job_type": "word_count",
    "payload": {"text": "hello world"},
}


async def test_create_and_get_job(client: AsyncClient) -> None:
    created = await client.post("/jobs", json=WORD_COUNT)
    assert created.status_code == 202
    body = created.json()
    job_id = body["id"]
    assert created.headers["location"] == f"/jobs/{job_id}"
    assert body["status"] == "QUEUED"
    assert body["priority"] == "NORMAL"
    assert body["payload"] == {"text": "hello world"}
    assert body["attempt_count"] == 0
    assert body["result"] is None
    assert body["error"] is None
    assert body["worker_id"] is None
    assert body["queued_at"] is not None
    assert body["run_after"] is None
    assert body["next_retry_at"] is None
    assert "idempotency_key" not in body

    fetched = await client.get(f"/jobs/{job_id}")
    assert fetched.status_code == 200
    assert fetched.json()["id"] == job_id
    assert fetched.json()["status"] == "QUEUED"

    settings = get_settings()
    with psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
    ) as conn:
        count = conn.execute("SELECT COUNT(*) FROM job_attempts").fetchone()
        assert count is not None
        assert count[0] == 0


async def test_unknown_job_returns_404(client: AsyncClient) -> None:
    missing = uuid.uuid4()
    response = await client.get(f"/jobs/{missing}")
    assert response.status_code == 404
    error = response.json()["error"]
    assert error["code"] == "JOB_NOT_FOUND"
    assert error["details"]["job_id"] == str(missing)


async def test_unknown_task_is_rejected(client: AsyncClient) -> None:
    response = await client.post(
        "/jobs",
        json={"job_type": "delete_everything", "payload": {}},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "UNKNOWN_JOB_TYPE"
    listed = await client.get("/jobs")
    assert listed.json()["total"] == 0


async def test_wrong_payload_is_rejected(client: AsyncClient) -> None:
    response = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": 123}},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "INVALID_PAYLOAD"
    listed = await client.get("/jobs")
    assert listed.json()["total"] == 0


async def test_delay_creates_scheduled_job(client: AsyncClient) -> None:
    response = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "hello"},
            "delay_seconds": 10,
        },
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "SCHEDULED"
    assert body["run_after"] is not None
    assert body["queued_at"] is None
    assert body["attempt_count"] == 0
    assert body["worker_id"] is None
    listed = await client.get("/jobs", params={"status": "SCHEDULED"})
    assert listed.json()["total"] == 1
    assert listed.json()["items"][0]["id"] == body["id"]


async def test_delay_above_max_rejected(client: AsyncClient) -> None:
    response = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "hello"},
            "delay_seconds": 604801,
        },
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "DELAY_TOO_LARGE"


async def test_negative_delay_rejected(client: AsyncClient) -> None:
    response = await client.post(
        "/jobs",
        json={
            "job_type": "word_count",
            "payload": {"text": "hello"},
            "delay_seconds": -1,
        },
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


async def test_invalid_priority(client: AsyncClient) -> None:
    response = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "x"}, "priority": "URGENT"},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


async def test_oversized_payload(client: AsyncClient) -> None:
    response = await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "x" * 40_000}},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "PAYLOAD_TOO_LARGE"


async def test_list_filters_pagination_and_order(client: AsyncClient) -> None:
    await client.post(
        "/jobs",
        json={"job_type": "word_count", "payload": {"text": "a"}, "priority": "LOW"},
    )
    await client.post(
        "/jobs",
        json={"job_type": "sleep", "payload": {"seconds": 1}, "priority": "HIGH"},
    )
    await client.post(
        "/jobs",
        json={"job_type": "sum_numbers", "payload": {"numbers": [1, 2]}, "priority": "HIGH"},
    )

    all_jobs = await client.get("/jobs")
    assert all_jobs.status_code == 200
    payload = all_jobs.json()
    assert payload["total"] == 3
    assert payload["limit"] == 50
    assert payload["offset"] == 0
    created_order = [item["created_at"] for item in payload["items"]]
    assert created_order == sorted(created_order, reverse=True)

    high = await client.get("/jobs", params={"priority": "HIGH"})
    assert high.json()["total"] == 2
    assert {item["job_type"] for item in high.json()["items"]} == {
        "sleep",
        "sum_numbers",
    }
    low = await client.get("/jobs", params={"priority": "LOW"})
    assert low.json()["total"] == 1
    critical = await client.get("/jobs", params={"priority": "CRITICAL"})
    assert critical.json()["total"] == 0
    normal = await client.get("/jobs", params={"priority": "NORMAL"})
    assert normal.json()["total"] == 0

    words = await client.get("/jobs", params={"job_type": "word_count"})
    assert words.json()["total"] == 1
    assert words.json()["items"][0]["payload"]["text"] == "a"

    queued = await client.get("/jobs", params={"status": "QUEUED"})
    assert queued.json()["total"] == 3

    page = await client.get("/jobs", params={"limit": 1, "offset": 0})
    assert len(page.json()["items"]) == 1
    assert page.json()["total"] == 3
    page2 = await client.get("/jobs", params={"limit": 1, "offset": 1})
    assert page2.json()["items"][0]["id"] != page.json()["items"][0]["id"]
    assert page2.json()["total"] == 3


async def test_negative_offset_rejected(client: AsyncClient) -> None:
    response = await client.get("/jobs", params={"offset": -1})
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


async def test_limit_too_large_rejected(client: AsyncClient) -> None:
    response = await client.get("/jobs", params={"limit": 101})
    assert response.status_code == 422


async def test_concurrent_creates(client: AsyncClient) -> None:
    async def submit(index: int) -> str:
        response = await client.post(
            "/jobs",
            json={"job_type": "word_count", "payload": {"text": f"job-{index}"}},
        )
        assert response.status_code == 202
        return str(response.json()["id"])

    ids = await asyncio.gather(*[submit(i) for i in range(20)])
    assert len(ids) == 20
    assert len(set(ids)) == 20
    listed = await client.get("/jobs", params={"limit": 100})
    assert listed.json()["total"] == 20
    assert {item["id"] for item in listed.json()["items"]} == set(ids)


async def test_persistence_across_app_instances(client: AsyncClient) -> None:
    created = await client.post("/jobs", json=WORD_COUNT)
    job_id = created.json()["id"]

    await dispose_engine()
    application = create_app()
    async with AsyncClient(
        transport=ASGITransport(app=application),
        base_url="http://test",
    ) as restarted:
        fetched = await restarted.get(f"/jobs/{job_id}")
        assert fetched.status_code == 200
        assert fetched.json()["id"] == job_id
        assert fetched.json()["payload"]["text"] == "hello world"
    await dispose_engine()


async def test_other_task_types_persist(client: AsyncClient) -> None:
    samples = [
        {"job_type": "sum_numbers", "payload": {"numbers": [1, 2, 3.5]}},
        {"job_type": "sleep", "payload": {"seconds": 0}},
        {"job_type": "prime_calculation", "payload": {"limit": 2}},
        {"job_type": "simulate_failure", "payload": {}},
    ]
    for sample in samples:
        response = await client.post("/jobs", json=sample)
        assert response.status_code == 202
        assert response.json()["status"] == "QUEUED"
        assert response.json()["job_type"] == sample["job_type"]
