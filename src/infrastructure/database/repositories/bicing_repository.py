from typing import List, Optional, Tuple, Dict
from sqlalchemy import select, func, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from src.domain.schemas.models import DBBicingStation

class BicingRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    async def get_nearby(
        self, 
        lat: float, 
        lon: float, 
        radius_km: float, 
        limit: int = 20
    ) -> List[Tuple[DBBicingStation, float]]:
        async with self.session_factory() as session:
            delta_lat = radius_km / 111.0
            delta_lon = radius_km / (111.0 * func.cos(func.radians(lat)))

            distance_expr = (
                6371.0 * func.acos(
                    func.cos(func.radians(lat)) * func.cos(func.radians(DBBicingStation.latitude)) * func.cos(func.radians(DBBicingStation.longitude) - func.radians(lon)) + 
                    func.sin(func.radians(lat)) * func.sin(func.radians(DBBicingStation.latitude))
                )
            ).label("distance_km")

            stmt = (
                select(DBBicingStation, distance_expr)
                .where(
                    and_(
                        DBBicingStation.latitude.between(lat - delta_lat, lat + delta_lat),
                        DBBicingStation.longitude.between(lon - delta_lon, lon + delta_lon)
                    )
                )
                .where(distance_expr <= radius_km)
                .order_by("distance_km")
                .limit(limit)
            )

            result = await session.execute(stmt)
            return result.all()

    async def upsert_all(self, stations_data: List[Dict]):
        if not stations_data:
            return

        async with self.session_factory() as session:
            stmt = pg_insert(DBBicingStation).values(stations_data)
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['id'],
                set_={
                    "name": stmt.excluded.name,
                    "latitude": stmt.excluded.latitude,
                    "longitude": stmt.excluded.longitude,
                    "slots": stmt.excluded.slots,
                    "mechanical_bikes": stmt.excluded.mechanical_bikes,
                    "electrical_bikes": stmt.excluded.electrical_bikes,
                    "availability": stmt.excluded.availability,
                    "last_updated": stmt.excluded.last_updated
                }
            )

            await session.execute(stmt)
            await session.commit()

    async def get_all(self) -> List[DBBicingStation]:
        async with self.session_factory() as session:
            result = await session.execute(select(DBBicingStation))
            return result.scalars().all()
        
    async def get_by_id(self, station_id: str) -> Optional[DBBicingStation]:
        async with self.session_factory() as session:
            # Forzamos str(station_id) por seguridad, ya que el modelo es String
            stmt = select(DBBicingStation).where(DBBicingStation.id == str(station_id))
            result = await session.execute(stmt)
            return result.scalars().first()