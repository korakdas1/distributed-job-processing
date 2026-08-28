"""FastAPI application factory and ASGI entrypoint."""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.trustedhost import TrustedHostMiddleware

from job_platform import __version__
from job_platform.api.errors import register_error_handlers
from job_platform.api.routes.health import router as health_router
from job_platform.api.routes.jobs import router as jobs_router
from job_platform.api.routes.metrics import router as metrics_router
from job_platform.api.routes.workers import router as workers_router
from job_platform.core.config import get_settings, validate_api_http_security
from job_platform.core.logging import configure_logging
from job_platform.db.session import dispose_engine
from job_platform.observability.middleware import ObservabilityMiddleware
from job_platform.queue.client import dispose_redis
from job_platform.security.context import SecurityContext


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level, service="api")
    yield
    await dispose_engine()
    await dispose_redis()


def create_app(*, clock: Callable[[], float] | None = None) -> FastAPI:
    settings = get_settings()
    validate_api_http_security(settings)
    security = SecurityContext.from_settings(settings, clock=clock)
    docs_url = "/docs" if security.docs_enabled else None
    application = FastAPI(
        title="Distributed Job Processing Platform",
        version=__version__,
        description=(
            "Accepts jobs over HTTP and stores them in PostgreSQL with a "
            "transactional outbox. A separate publisher dispatches to Redis Streams. "
            "Independent workers execute allowlisted handlers and report liveness "
            "via Redis TTL heartbeats. GET /workers overlays that heartbeat on "
            "durable worker history. GET /metrics and GET /metrics/summary observe "
            "the system without changing job-processing semantics. The API does not "
            "run jobs or call Redis on POST /jobs. Protected routes use a static "
            "Bearer API key (VIEWER or OPERATOR), not OAuth or JWT."
        ),
        lifespan=lifespan,
        docs_url=docs_url,
        redoc_url="/redoc" if security.docs_enabled else None,
        openapi_url="/openapi.json" if security.docs_enabled else None,
    )
    application.state.security = security
    register_error_handlers(application)
    application.add_middleware(ObservabilityMiddleware)
    if settings.is_production:
        application.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=list(security.allowed_hosts),
        )
    application.include_router(health_router)
    application.include_router(jobs_router)
    application.include_router(workers_router)
    application.include_router(metrics_router)
    return application


app = create_app()
