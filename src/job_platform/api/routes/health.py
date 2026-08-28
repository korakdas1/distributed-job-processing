"""Liveness and dependency readiness.

API readiness means this process can durably accept jobs (PostgreSQL).
Redis is observed for degradation only; Redis down does not return 503.
GET /health does not query PostgreSQL or Redis.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from job_platform.db.session import get_engine
from job_platform.queue.client import ping_redis

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])

_REDIS_DOWN_REASON = "Redis unavailable; jobs can be accepted durably but dispatch is degraded."


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "api"}


async def _redis_up() -> bool:
    try:
        await ping_redis()
    except RedisError:
        return False
    return True


@router.get("/ready")
async def ready() -> JSONResponse:
    postgres_up = True
    try:
        engine = get_engine()
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except SQLAlchemyError:
        logger.exception("PostgreSQL readiness check failed")
        postgres_up = False
    redis_up = await _redis_up()
    dependencies = {
        "postgres": "up" if postgres_up else "down",
        "redis": "up" if redis_up else "down",
    }
    checks = {"postgres": "ok" if postgres_up else "error"}
    if postgres_up:
        content: dict[str, object] = {
            "status": "ready",
            "degraded": not redis_up,
            "dependencies": dependencies,
            "checks": checks,
        }
        if not redis_up:
            content["reason"] = _REDIS_DOWN_REASON
        return JSONResponse(status_code=200, content=content)
    return JSONResponse(
        status_code=503,
        content={
            "status": "not_ready",
            "degraded": True,
            "dependencies": dependencies,
            "checks": checks,
        },
    )
