from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.domain.schemas.models import DBLine


class LineRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    async def get_all(self, transport_type: str = None) -> List[DBLine]:
        async with self.session_factory() as session:
            stmt = select(DBLine)
            if transport_type:
                stmt = stmt.where(DBLine.transport_type == transport_type)
            
            result = await session.execute(stmt)
            return result.scalars().all()

    async def get_by_id(self, line_id: str) -> Optional[DBLine]:
        async with self.session_factory() as session:
            stmt = select(DBLine).where(DBLine.id == line_id)
            result = await session.execute(stmt)
            return result.scalars().first()
    
    async def get_by_code(self, code: str) -> Optional[DBLine]:
        async with self.session_factory() as session:
            query = select(DBLine).where(DBLine.code == code)
            result = await session.execute(query)
            return result.scalars().first()
        
    async def get_by_transport_type(self, transport_type: str) -> List[DBLine]:
        async with self.session_factory() as session:
            query = select(DBLine).where(DBLine.transport_type == transport_type)
            result = await session.execute(query)
            return result.scalars().all()

    async def upsert_many(self, lines: List[DBLine]):
        async with self.session_factory() as session:
            for line in lines:
                await session.merge(line)
            
            await session.commit()