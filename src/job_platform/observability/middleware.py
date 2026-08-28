"""API request correlation and HTTP metric observation.

Request IDs are generated server-side and are local to this API process.
They are not distributed traces and are not stored on jobs.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable

from starlette.datastructures import MutableHeaders
from starlette.types import Message, Receive, Scope, Send

from job_platform.core.logging import log_event
from job_platform.observability.metrics import observe_http_request

logger = logging.getLogger(__name__)

UNMATCHED_ROUTE = "unmatched"
_SAFE_500_MESSAGE = "An unexpected internal error occurred."


def route_template_from_scope(scope: Scope) -> str:
    route = scope.get("route")
    path = getattr(route, "path", None)
    if isinstance(path, str) and path:
        return path
    return UNMATCHED_ROUTE


def _attach_request_id(scope: Scope, request_id: str) -> None:
    state = scope.setdefault("state", {})
    if isinstance(state, dict):
        state["request_id"] = request_id
    else:
        state.request_id = request_id


class ObservabilityMiddleware:
    def __init__(self, app: Callable[[Scope, Receive, Send], Awaitable[None]]) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request_id = uuid.uuid4().hex
        _attach_request_id(scope, request_id)
        method = str(scope.get("method") or "GET").upper()
        started = time.perf_counter()
        status_code = 500
        started_response = False

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, started_response
            if message["type"] == "http.response.start":
                started_response = True
                status_code = int(message["status"])
                headers = MutableHeaders(raw=message.setdefault("headers", []))
                headers["x-request-id"] = request_id
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            logger.exception(
                "event=unhandled_asgi_error request_id=%s method=%s",
                request_id,
                method,
            )
            if started_response:
                raise
            payload = json.dumps(
                {
                    "error": {
                        "code": "INTERNAL_SERVER_ERROR",
                        "message": _SAFE_500_MESSAGE,
                        "details": {},
                        "request_id": request_id,
                    }
                }
            ).encode("utf-8")
            await send_wrapper(
                {
                    "type": "http.response.start",
                    "status": 500,
                    "headers": [(b"content-type", b"application/json")],
                }
            )
            await send_wrapper({"type": "http.response.body", "body": payload})
        finally:
            duration = time.perf_counter() - started
            route = route_template_from_scope(scope)
            if started_response:
                observe_http_request(
                    method=method,
                    route=route,
                    status_code=status_code,
                    duration_seconds=duration,
                )
                log_event(
                    logger,
                    "http_request_completed",
                    request_id=request_id,
                    method=method,
                    route=route,
                    status_code=status_code,
                    duration_ms=round(duration * 1000, 3),
                )
