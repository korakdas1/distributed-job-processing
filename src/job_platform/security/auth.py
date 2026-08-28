"""Bearer API-key authentication. Compare SHA-256 digests with compare_digest."""

from __future__ import annotations

import hmac
import logging

from fastapi.security import HTTPAuthorizationCredentials
from starlette.requests import Request

from job_platform.core.errors import AppError
from job_platform.core.logging import log_event
from job_platform.observability.metrics import observe_auth_failure
from job_platform.observability.middleware import route_template_from_scope
from job_platform.security.context import SecurityContext, hash_api_key
from job_platform.security.principal import (
    ANONYMOUS,
    OPERATOR_PRINCIPAL,
    VIEWER_PRINCIPAL,
    Principal,
)

logger = logging.getLogger(__name__)

WWW_AUTHENTICATE = {"WWW-Authenticate": "Bearer"}


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def authenticate(
    request: Request,
    context: SecurityContext,
    credentials: HTTPAuthorizationCredentials | None,
) -> Principal:
    if not context.auth_enabled:
        return ANONYMOUS
    token = credentials.credentials if credentials is not None else ""
    if credentials is None or credentials.scheme.lower() != "bearer" or token == "":
        observe_auth_failure("missing")
        log_event(
            logger,
            "authentication_failed",
            method=request.method,
            route=route_template_from_scope(request.scope),
            reason="missing",
            request_id=_request_id(request),
        )
        raise AppError(
            "AUTHENTICATION_REQUIRED",
            "Authentication is required.",
            status_code=401,
            headers=WWW_AUTHENTICATE,
        )
    presented = hash_api_key(token)
    viewer_ok = hmac.compare_digest(presented, context.viewer_digest)
    operator_ok = hmac.compare_digest(presented, context.operator_digest)
    if viewer_ok:
        return VIEWER_PRINCIPAL
    if operator_ok:
        return OPERATOR_PRINCIPAL
    observe_auth_failure("invalid")
    log_event(
        logger,
        "authentication_failed",
        method=request.method,
        route=route_template_from_scope(request.scope),
        reason="invalid",
        request_id=_request_id(request),
    )
    raise AppError(
        "INVALID_API_KEY",
        "The provided API key is not valid.",
        status_code=401,
        headers=WWW_AUTHENTICATE,
    )
