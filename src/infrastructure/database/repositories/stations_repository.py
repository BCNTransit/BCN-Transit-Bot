from typing import List, Optional, Tuple
from sqlalchemy.orm import selectinload, joinedload
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from src.domain.enums.transport_type import TransportType
from src.domain.schemas.models import DBLine, DBPhysicalStation, DBRouteStop

class StationsRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory
        
    async def get_by_transport_type(self, transport_type: str) -> List[DBPhysicalStation]:
        """
        Obtiene las estaciones FÍSICAS para pintar en el mapa.
        Usamos DBPhysicalStation porque contiene lat, lon y el resumen de líneas.
        """
        async with self.session_factory() as session:
            stmt = (
                select(DBPhysicalStation)
                .where(DBPhysicalStation.transport_type == transport_type)
                # No necesitamos cargar 'route_stops' aquí, con 'lines_summary' basta para el mapa
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
        Obtiene el detalle de una estación física (ej: al hacer click en el mapa).
        Cargamos también qué líneas paran ahí detalladamente.
        """
        async with self.session_factory() as session:
            stmt = (
                select(DBPhysicalStation)
                .where(DBPhysicalStation.id == station_id)
                # Opcional: Cargar todas las paradas de ruta asociadas para ver detalles
                # de origen/destino de las líneas que pasan por aquí.
                .options(selectinload(DBPhysicalStation.route_stops))
            )
            result = await session.execute(stmt)
            return result.scalars().first()
        
    async def get_by_code(self, code: str, transport_type: str) -> Optional[DBPhysicalStation]:
        """
        Busca una estación física a través de sus paradas de ruta.
        Si la estación tiene 3 líneas (L1, L3, L5), buscar por el código 
        de CUALQUIERA de ellas devolverá la estación física correcta.
        """
        async with self.session_factory() as session:
            stmt = (
                select(DBPhysicalStation)
                # 1. Unimos con la tabla de paradas
                .join(DBRouteStop, DBPhysicalStation.id == DBRouteStop.physical_station_id)
                # 2. Filtramos por el código externo en la tabla hija
                .where(DBRouteStop.station_external_code == code)
                # 3. Filtramos por tipo de transporte en la tabla padre
                .where(DBPhysicalStation.transport_type == transport_type)
            )
            
            result = await session.execute(stmt)
            return result.scalars().first()

    async def get_all_raw(self) -> List[DBPhysicalStation]:
        """
        Devuelve todas las estaciones físicas (útil para sitemaps o debug).
        """
        async with self.session_factory() as session:
            stmt = select(DBPhysicalStation)
            result = await session.execute(stmt)
            return result.scalars().all()

    async def get_nearby(
        self, 
        lat: float, 
        lon: float, 
        radius_km: float, 
        transport_type: Optional[str] = None,
        limit: int = 50
    ) -> List[Tuple[DBPhysicalStation, float]]:
        """
        Busca estaciones cercanas usando SQL.
        Retorna una lista de tuplas (Estación, Distancia_KM).
        """
        async with self.session_factory() as session:
            # 1. Bounding Box (Filtro rápido por índice de lat/lon)
            # 1 grado latitud ≈ 111km
            delta_lat = radius_km / 111.0
            delta_lon = radius_km / (111.0 * func.cos(func.radians(lat)))
            
            # 2. Definir la fórmula Haversine en SQL
            # 6371 es el radio de la Tierra en km
            distance_expr = (
                6371.0 * func.acos(
                    func.cos(func.radians(lat)) * func.cos(func.radians(DBPhysicalStation.latitude)) * func.cos(func.radians(DBPhysicalStation.longitude) - func.radians(lon)) + 
                    func.sin(func.radians(lat)) * func.sin(func.radians(DBPhysicalStation.latitude))
                )
            ).label("distance_km")

            # 3. Construir Query
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

            # Filtro opcional por transporte
            if transport_type:
                stmt = stmt.where(DBPhysicalStation.transport_type == transport_type)

            # Ordenar por cercanía y limitar
            stmt = stmt.order_by("distance_km").limit(limit)

            result = await session.execute(stmt)
            # Retorna [(DBPhysicalStation, 0.45), (DBPhysicalStation, 1.2)...]
            return result.all()