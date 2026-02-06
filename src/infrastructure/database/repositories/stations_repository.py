from typing import List, Optional, Tuple
from sqlalchemy.orm import selectinload, joinedload
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from src.domain.schemas.models import DBPhysicalStation, DBRouteStop

class StationsRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory
        
    async def get_by_transport_type(self, transport_type: str) -> List[DBPhysicalStation]:
        """
        Obtiene las estaciones FÍSICAS para pintar en el mapa.
        """
        async with self.session_factory() as session:
            stmt = (
                select(DBPhysicalStation)
                .where(DBPhysicalStation.transport_type == transport_type)
            )
            result = await session.execute(stmt)
            return result.scalars().all()
        
    async def get_route_stops_with_lines(self, transport_type: str) -> List[DBRouteStop]:
        async with self.session_factory() as session:
            stmt = (
                select(DBRouteStop)
                .join(DBRouteStop.line)
                .join(DBRouteStop.station)
                .options(
                    joinedload(DBRouteStop.line),
                    joinedload(DBRouteStop.station)
                )
                .where(
                    and_(
                        DBPhysicalStation.transport_type == transport_type
                    )
                )
            )
            result = await session.execute(stmt)
            return result.scalars().all()

    async def get_by_line_id(self, line_db_id: str) -> List[DBRouteStop]:
        async with self.session_factory() as session:
            stmt = (
                select(DBRouteStop)
                .where(DBRouteStop.line_id == line_db_id)
                .order_by(DBRouteStop.order)
                .options(
                    joinedload(DBRouteStop.station),                    
                    selectinload(DBRouteStop.line) 
                )
            )
            result = await session.execute(stmt)
            return result.scalars().all()

    async def get_stop_by_physical_and_line_id(self, physical_id: str, line_id: str) -> Optional[DBRouteStop]:
        async with self.session_factory() as session:
            stmt = (
                select(DBRouteStop)
                .options(joinedload(DBRouteStop.station))
                .where(DBRouteStop.physical_station_id == physical_id)
                .where(DBRouteStop.line_id == line_id)
            )
            result = await session.execute(stmt)
            return result.scalars().first()
        
    async def get_by_id(self, station_id: str) -> Optional[DBPhysicalStation]:
        """
        Obtiene el detalle de una estación física.
        """
        async with self.session_factory() as session:
            stmt = (
                select(DBPhysicalStation)
                .where(DBPhysicalStation.id == station_id)
                .options(selectinload(DBPhysicalStation.route_stops))
            )
            result = await session.execute(stmt)
            return result.scalars().first()
        
    async def get_by_code(self, code: str, transport_type: str) -> Optional[DBPhysicalStation]:
        """
        Busca una estación física a través del código externo de cualquiera de sus paradas.
        """
        async with self.session_factory() as session:
            stmt = (
                select(DBPhysicalStation)
                .join(DBRouteStop, DBPhysicalStation.id == DBRouteStop.physical_station_id)
                .where(DBRouteStop.station_external_code == code)
                .where(DBPhysicalStation.transport_type == transport_type)
            )
            
            result = await session.execute(stmt)
            return result.scalars().first()

    async def get_nearby(
        self, 
        lat: float, 
        lon: float, 
        radius_km: float, 
        transport_type: Optional[str] = None,
        limit: int = 50
    ) -> List[Tuple[DBPhysicalStation, float]]:
        """
        Busca estaciones cercanas (Haversine).
        """
        async with self.session_factory() as session:
            # Cálculos trigonométricos
            delta_lat = radius_km / 111.0
            # Corrección del delta_lon (seguridad si cos(lat) es 0, aunque raro en BCN)
            cos_lat = func.cos(func.radians(lat))
            delta_lon = radius_km / (111.0 * cos_lat)
            
            # Fórmula Haversine en SQL
            distance_expr = (
                6371.0 * func.acos(
                    func.cos(func.radians(lat)) * func.cos(func.radians(DBPhysicalStation.latitude)) * func.cos(func.radians(DBPhysicalStation.longitude) - func.radians(lon)) + 
                    func.sin(func.radians(lat)) * func.sin(func.radians(DBPhysicalStation.latitude))
                )
            ).label("distance_km")

            stmt = (
                select(DBPhysicalStation, distance_expr)
                .where(
                    and_(
                        DBPhysicalStation.latitude.between(lat - delta_lat, lat + delta_lat),
                        DBPhysicalStation.longitude.between(lon - delta_lon, lon + delta_lon)
                    )
                )
                .where(distance_expr <= radius_km)
            )

            if transport_type:
                stmt = stmt.where(DBPhysicalStation.transport_type == transport_type)

            stmt = stmt.order_by("distance_km").limit(limit)

            result = await session.execute(stmt)
            return result.all()