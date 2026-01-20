from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from src.domain.schemas.models import StationModel

class StationsRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory
        
    async def get_by_transport_type(self, transport_type: str) -> List[StationModel]:
        async with self.session_factory() as session:
            stmt = (
                select(StationModel)
                .where(StationModel.transport_type == transport_type)
                .options(selectinload(StationModel.line))
            )
            result = await session.execute(stmt)
            return result.scalars().all()

    async def get_by_line_id(self, line_db_id: str) -> List[StationModel]:
        async with self.session_factory() as session:
            stmt = (
                select(StationModel)
                .where(StationModel.line_id == line_db_id)
                .order_by(StationModel.order)
                .options(selectinload(StationModel.line))
            )
            result = await session.execute(stmt)
            return result.scalars().all()

    async def get_by_id(self, station_id: str) -> Optional[StationModel]:
        async with self.session_factory() as session:
            stmt = select(StationModel).where(StationModel.id == station_id)
            result = await session.execute(stmt)
            return result.scalars().first()

    async def upsert_many(self, stations: List[StationModel]):
        if not stations:
            return

        async with self.session_factory() as session:
            valid_columns = {c.name for c in StationModel.__table__.columns}

            stations_data = []
            for s in stations:
                data = {
                    k: v for k, v in s.__dict__.items() 
                    if k in valid_columns
                }
                stations_data.append(data)

            stmt = insert(StationModel).values(stations_data)

            update_dict = {
                col.name: stmt.excluded[col.name]
                for col in StationModel.__table__.columns
                if col.name != 'id'
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=['id'],
                set_=update_dict
            )

            await session.execute(stmt)
            await session.commit()

    async def get_all_raw(self) -> List[StationModel]:
        async with self.session_factory() as session:
            stmt = (
                select(StationModel).options(selectinload(StationModel.line)) 
            )
            
            result = await session.execute(stmt)
            return result.scalars().all()