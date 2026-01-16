from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from src.domain.schemas.models import LineModel

class LineRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all(self) -> List[LineModel]:
        stmt = select(LineModel)
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def get_by_id(self, line_id: str) -> Optional[LineModel]:
        stmt = select(LineModel).where(LineModel.id == line_id)
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def get_by_transport_type(self, transport_type: str) -> List[LineModel]:
        stmt = select(LineModel).where(LineModel.transport_type == transport_type)
        result = await self.session.execute(stmt)
        return result.scalars().all()