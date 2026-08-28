"""Job HTTP routes. PostgreSQL is authoritative; Redis is not called."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from job_platform.api.dependencies import db_session, settings_dep
from job_platform.api.services import (
    get_job_by_id,
    list_jobs,
    request_job_cancellation,
    submit_job,
)
from job_platform.core.config import Settings
from job_platform.core.enums import JobPriority, JobStatus, JobType
from job_platform.core.errors import AppError
from job_platform.models.job import Job
from job_platform.schemas.job import JobCreateRequest, JobListResponse, JobRead
from job_platform.security.dependencies import require_operator, require_viewer
from job_platform.security.principal import Principal

router = APIRouter(tags=["jobs"])

SessionDep = Annotated[AsyncSession, Depends(db_session)]
SettingsDep = Annotated[Settings, Depends(settings_dep)]
ViewerDep = Annotated[Principal, Depends(require_viewer)]
OperatorDep = Annotated[Principal, Depends(require_operator)]


@router.post("/jobs", status_code=status.HTTP_202_ACCEPTED, response_model=JobRead)
async def create_job(
    body: JobCreateRequest,
    response: Response,
    session: SessionDep,
    settings: SettingsDep,
    _principal: OperatorDep,
    idempotency_key: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            description="Optional client token. Same key and request replay the same job.",
        ),
    ] = None,
) -> Job:
    result = await submit_job(
        session,
        settings=settings,
        body=body,
        idempotency_key=idempotency_key,
    )
    response.headers["Location"] = f"/jobs/{result.job.id}"
    if result.replayed:
        response.headers["Idempotency-Replayed"] = "true"
    return result.job


@router.get("/jobs/{job_id}", response_model=JobRead)
async def read_job(job_id: uuid.UUID, session: SessionDep, _principal: ViewerDep) -> Job:
    job = await get_job_by_id(session, job_id)
    if job is None:
        raise AppError(
            "JOB_NOT_FOUND",
            "No job exists with the given id.",
            status_code=404,
            details={"job_id": str(job_id)},
        )
    return job


@router.delete(
    "/jobs/{job_id}",
    response_model=JobRead,
    responses={
        200: {"description": "Waiting job cancelled, or already CANCELLED."},
        202: {"description": "Cancellation requested while the job is RUNNING."},
        404: {"description": "Job not found."},
        409: {"description": "Job is SUCCEEDED or FAILED."},
    },
)
async def cancel_job(
    job_id: uuid.UUID, session: SessionDep, _principal: OperatorDep
) -> JSONResponse:
    job, status_code = await request_job_cancellation(session, job_id)
    return JSONResponse(
        status_code=status_code,
        content=jsonable_encoder(JobRead.model_validate(job)),
    )


@router.get("/jobs", response_model=JobListResponse)
async def list_job_records(
    session: SessionDep,
    _principal: ViewerDep,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    status_filter: Annotated[JobStatus | None, Query(alias="status")] = None,
    job_type: JobType | None = None,
    priority: JobPriority | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
) -> JobListResponse:
    rows, total = await list_jobs(
        session,
        limit=limit,
        offset=offset,
        status=status_filter,
        job_type=job_type,
        priority=priority,
        created_after=created_after,
        created_before=created_before,
    )
    return JobListResponse(
        items=[JobRead.model_validate(row) for row in rows],
        limit=limit,
        offset=offset,
        total=total,
    )
