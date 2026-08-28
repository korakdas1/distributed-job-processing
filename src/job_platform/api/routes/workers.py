"""Worker registry and liveness. History is PostgreSQL; presence is Redis TTL."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from job_platform.api.dependencies import db_session
from job_platform.api.worker_services import list_workers
from job_platform.schemas.worker import WorkerListResponse
from job_platform.security.dependencies import require_viewer
from job_platform.security.principal import Principal

router = APIRouter(tags=["workers"])

SessionDep = Annotated[AsyncSession, Depends(db_session)]
ViewerDep = Annotated[Principal, Depends(require_viewer)]


@router.get("/workers", response_model=WorkerListResponse)
async def read_workers(
    session: SessionDep,
    _principal: ViewerDep,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> WorkerListResponse:
    return await list_workers(session, limit=limit, offset=offset)
