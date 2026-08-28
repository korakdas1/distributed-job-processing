"""FastAPI dependencies."""

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession

from job_platform.core.config import Settings, get_settings
from job_platform.db.session import get_session_factory


def settings_dep() -> Settings:
    return get_settings()


async def db_session() -> AsyncIterator[AsyncSession]:
    """Own session lifetime only. Write routes commit explicitly after work."""
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
