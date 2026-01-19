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
            # 1. Convertir Objetos a Diccionarios
            # SQLAlchemy Core necesita dicts, no objetos de modelo.
            # Filtramos '_sa_instance_state' que es basura interna de SQLAlchemy.
            stations_data = [
                {k: v for k, v in s.__dict__.items() if not k.startswith('_sa_')}
                for s in stations
            ]

            # 2. Preparar la sentencia INSERT
            stmt = insert(StationModel).values(stations_data)

            # 3. Configurar el "ON CONFLICT" (El Upsert real)
            # Si el ID ya existe, actualiza todos los campos excepto el ID.
            update_dict = {
                col.name: col 
                for col in stmt.excluded 
                if col.name != 'id' # No actualizamos la Primary Key
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=['id'], # La columna que causa el conflicto (PK)
                set_=update_dict
            )

            # 4. Ejecutar UNA sola query
            await session.execute(stmt)
            await session.commit()