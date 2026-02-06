from datetime import datetime
from typing import List, Optional, Set
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import or_, select, func, and_, update
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from src.domain.schemas.models import DBAlert


class AlertsRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    async def get_active_alerts(self, transport_type: Optional[str] = None) -> List[DBAlert]:
        async with self.session_factory() as session:
            now = datetime.now()
            
            conditions = [
                DBAlert.active == True,
                DBAlert.begin_date <= now,
                or_(
                    DBAlert.end_date == None,
                    DBAlert.end_date > now
                )
            ]
            
            if transport_type:
                conditions.append(DBAlert.transport_type == transport_type)
                
            stmt = select(DBAlert).where(and_(*conditions)).order_by(DBAlert.begin_date.desc())
            result = await session.execute(stmt)
            return result.scalars().all()

    async def get_affected_line_names(self, transport_type: str) -> Set[str]:
        now = datetime.utcnow()
        
        async with self.session_factory() as session:
            stmt = (
                select(
                    func.distinct(
                        func.jsonb_array_elements(DBAlert.affected_entities)
                        .op('->>')('line_name')
                    )
                )
                .where(DBAlert.transport_type == transport_type)
                .where(DBAlert.active == True)
                .where(
                    and_(
                        DBAlert.begin_date <= now,
                        or_(
                            DBAlert.end_date >= now,
                            DBAlert.end_date.is_(None)
                        )
                    )
                )
            )
            
            result = await session.execute(stmt)
            
            return {row[0] for row in result.all() if row[0]}
        
    async def upsert_many(self, alerts: List[DBAlert]):
        if not alerts:
            return

        values_list = []
        for alert in alerts:
            values_list.append({
                "external_id": alert.external_id,
                "transport_type": alert.transport_type,
                "begin_date": alert.begin_date,
                "end_date": alert.end_date,
                "status": alert.status,
                "cause": alert.cause,
                "publications": alert.publications,
                "active": True,
                "affected_entities": alert.affected_entities
            })
        async with self.session_factory() as session:
            stmt = insert(DBAlert).values(values_list)

            stmt = stmt.on_conflict_do_update(
                index_elements=['external_id'],
                set_={
                    "active": True,
                    "end_date": stmt.excluded.end_date,
                    "status": stmt.excluded.status,
                    "cause": stmt.excluded.cause,
                    "publications": stmt.excluded.publications,
                    "affected_entities": stmt.excluded.affected_entities
                }
            )

            await session.execute(stmt)
            await session.commit()

    async def mark_all_as_inactive(self, transport_type: str):
        async with self.session_factory() as session:
            stmt = (
                update(DBAlert)
                .where(DBAlert.transport_type == transport_type)
                .values(active=False)
            )
            
            try:
                await session.execute(stmt)
                await session.commit()
            except Exception as e:
                await session.rollback()

    