from typing import Optional

from sqlalchemy import select
from src.domain.schemas.models import DBAppVersion
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

class AppVersionRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    async def get_by_platform(self, platform: str) -> Optional[DBAppVersion]:
        async with self.session_factory() as session:
            query = select(DBAppVersion).where(DBAppVersion.platform == platform)            
            result = await session.execute(query)
            return result.scalar_one_or_none()