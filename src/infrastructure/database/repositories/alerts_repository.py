from datetime import datetime
from typing import Set
from sqlalchemy import or_, select, func, and_
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from src.domain.schemas.models import Alert

class AlertsRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    async def get_affected_line_names(self, transport_type: str) -> Set[str]:
        now = datetime.utcnow()
        
        async with self.session_factory() as session:
            stmt = (
                select(
                    func.distinct(
                        func.jsonb_array_elements(Alert.affected_entities)
                        .op('->>')('line_name')
                    )
                )
                .where(Alert.transport_type == transport_type)
                .where(
                    and_(
                        Alert.begin_date <= now,
                        or_(
                            Alert.end_date >= now,
                            Alert.end_date.is_(None)
                        )
                    )
                )
            )
            
            result = await session.execute(stmt)
            
            return {row[0] for row in result.all() if row[0]}