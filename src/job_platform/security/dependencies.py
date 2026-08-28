"""Reusable FastAPI dependencies for VIEWER and OPERATOR access."""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from job_platform.core.errors import AppError
from job_platform.core.logging import log_event
from job_platform.observability.metrics import observe_authorization_denied, observe_rate_limited
from job_platform.observability.middleware import route_template_from_scope
from job_platform.security.auth import authenticate
from job_platform.security.context import SecurityContext
from job_platform.security.principal import Principal, Role
from job_platform.security.rate_limit import LimitClass

logger = logging.getLogger(__name__)

BEARER_SCHEME = HTTPBearer(
    auto_error=False,
    scheme_name="ApiKey",
    description="Static API key sent as Authorization: Bearer <key>. Not an OAuth token.",
)


def get_security_context(request: Request) -> SecurityContext:
    context = getattr(request.app.state, "security", None)
    if not isinstance(context, SecurityContext):
        raise RuntimeError("security context is not installed on the application")
    return context


def _enforce_rate_limit(
    request: Request,
    context: SecurityContext,
    principal: Principal,
    limit_class: LimitClass,
) -> None:
    if not context.rate_limit_enabled:
        return
    allowed, retry_after = context.limiter.consume(principal.name, limit_class)
    if allowed:
        return
    observe_rate_limited(limit_class.value)
    log_event(
        logger,
        "rate_limit_exceeded",
        method=request.method,
        route=route_template_from_scope(request.scope),
        role=principal.role.value if principal.role is not None else principal.name,
        limit_class=limit_class.value,
        request_id=getattr(request.state, "request_id", None),
    )
    raise AppError(
        "RATE_LIMIT_EXCEEDED",
        "Too many requests.",
        status_code=429,
        details={"limit_class": limit_class.value},
        headers={"Retry-After": str(retry_after)},
    )


def _deny(request: Request, principal: Principal) -> None:
    observe_authorization_denied(
        principal.role.value if principal.role is not None else principal.name
    )
    log_event(
        logger,
        "authorization_denied",
        method=request.method,
        route=route_template_from_scope(request.scope),
        role=principal.role.value if principal.role is not None else principal.name,
        reason="INSUFFICIENT_PERMISSION",
        request_id=getattr(request.state, "request_id", None),
    )
    raise AppError(
        "INSUFFICIENT_PERMISSION",
        "The authenticated principal is not allowed to perform this action.",
        status_code=403,
    )


async def require_viewer(
    request: Request,
    context: Annotated[SecurityContext, Depends(get_security_context)],
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(BEARER_SCHEME)],
) -> Principal:
    principal = authenticate(request, context, credentials)
    if context.auth_enabled and principal.role not in {Role.VIEWER, Role.OPERATOR}:
        _deny(request, principal)
    _enforce_rate_limit(request, context, principal, LimitClass.READ)
    return principal


async def require_operator(
    request: Request,
    context: Annotated[SecurityContext, Depends(get_security_context)],
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(BEARER_SCHEME)],
) -> Principal:
    principal = authenticate(request, context, credentials)
    if context.auth_enabled and principal.role is not Role.OPERATOR:
        _deny(request, principal)
    _enforce_rate_limit(request, context, principal, LimitClass.WRITE)
    return principal
