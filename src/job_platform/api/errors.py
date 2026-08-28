"""HTTP error envelope and FastAPI exception handlers."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException as StarletteHTTPException

from job_platform.core.errors import AppError

logger = logging.getLogger(__name__)


def error_body(
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
    request_id: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "error": {"code": code, "message": message, "details": details or {}}
    }
    if request_id is not None:
        payload["error"]["request_id"] = request_id
    return payload


def error_response(
    status_code: int,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
    request_id: str | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=error_body(code, message, details, request_id=request_id),
        headers=headers or None,
    )


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def app_error_handler(_request: Request, exc: AppError) -> JSONResponse:
        return error_response(
            exc.status_code,
            exc.code,
            exc.message,
            exc.details,
            headers=exc.headers or None,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        return error_response(
            422,
            "VALIDATION_ERROR",
            "Request validation failed.",
            {"errors": jsonable_encoder(exc.errors())},
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(
        _request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        code = "HTTP_ERROR"
        if exc.status_code == 404:
            code = "NOT_FOUND"
        elif exc.status_code == 405:
            code = "METHOD_NOT_ALLOWED"
        detail = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
        return error_response(exc.status_code, code, detail)

    @app.exception_handler(SQLAlchemyError)
    async def sqlalchemy_handler(_request: Request, exc: SQLAlchemyError) -> JSONResponse:
        logger.exception("Database error: %s", exc.__class__.__name__)
        return error_response(
            503,
            "DATABASE_UNAVAILABLE",
            "The database is temporarily unavailable.",
        )

    @app.exception_handler(Exception)
    async def unhandled_handler(request: Request, exc: Exception) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)
        logger.exception(
            "Unhandled error: %s request_id=%s",
            exc.__class__.__name__,
            request_id,
        )
        return error_response(
            500,
            "INTERNAL_SERVER_ERROR",
            "An unexpected internal error occurred.",
            request_id=request_id if isinstance(request_id, str) else None,
        )
