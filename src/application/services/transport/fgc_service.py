import asyncio
import time
from typing import List, Optional

# Domain Models
from src.domain.models.common.station import Station
from src.domain.models.common.line import Line
from src.domain.models.common.line_route import LineRoute
from src.domain.models.common.next_trip import NextTrip, normalize_to_seconds
from src.domain.models.common.alert import Alert
from src.domain.enums.transport_type import TransportType

# Infrastructure & App
from src.infrastructure.external.api.fgc_api_service import FgcApiService
from src.application.services.user_data_manager import UserDataManager
from src.infrastructure.localization.language_manager import LanguageManager
from src.application.services.cache_service import CacheService
from src.core.logger import logger
from .service_base import ServiceBase

class FgcService(ServiceBase):
    """
    Servicio para gestionar datos de FGC.
    Incluye lógica de enriquecimiento (Geocoding/Mou-te) para obtener IDs de tiempo real.
    """

    def __init__(self, 
                 fgc_api_service: FgcApiService,
                 language_manager: LanguageManager,
                 cache_service: CacheService = None,
                 user_data_manager: UserDataManager = None):
        super().__init__(cache_service, user_data_manager)
        self.fgc_api_service = fgc_api_service
        self.language_manager = language_manager
        logger.info(f"[{self.__class__.__name__}] FgcService initialized")

    # =========================================================================
    # 🔄 SYNC & FETCH IMPLEMENTATIONS
    # =========================================================================

    async def sync_lines(self):
        await super().sync_lines(TransportType.FGC)

    async def sync_stations(self):
        await super().sync_stations(TransportType.FGC)

    async def fetch_lines(self) -> List[Line]:
        return await self.fgc_api_service.get_all_lines()

    async def fetch_stations(self) -> List[Station]:
        lines = await self.line_repository.get_all(TransportType.FGC.value)
        if not lines:
            lines = await self.fetch_lines()

        semaphore_fetch = asyncio.Semaphore(5)
        
        async def fetch_safe(line_id):
            async with semaphore_fetch:
                try:
                    return await self.fetch_stations_by_line(line_id)
                except Exception as e:
                    logger.error(f"Error fetching FGC line {line_id}: {e}")
                    return []

        raw_results = await asyncio.gather(*[fetch_safe(l.original_id or l.id) for l in lines])
        all_raw_stations = [s for sublist in raw_results for s in sublist]

        logger.info(f"🌍 Calculando IDs de tiempo real (Moute) para {len(all_raw_stations)} estaciones FGC...")        
        semaphore_geo = asyncio.Semaphore(10)

        async def enrich_station_safe(station: Station):
            async with semaphore_geo:
                try:
                    moute_data = await self.fgc_api_service.get_near_stations(station.latitude, station.longitude)
                    
                    if moute_data:                    
                        station.moute_id = moute_data[0].get("id")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error enriqueciendo estación FGC {station.name}: {e}")
                
                return station

        enriched_stations = await asyncio.gather(*[enrich_station_safe(s) for s in all_raw_stations])
        
        return enriched_stations

    async def fetch_stations_by_line(self, line_code: str) -> List[Station]:
        return await self.fgc_api_service.get_stations_by_line(line_code)

    async def fetch_alerts(self) -> List[Alert]:
        # TODO: Conectar con API real si FGC provee endpoint de incidencias
        return []

    # =========================================================================
    # 🔍 MÉTODOS DE LECTURA (APP)
    # =========================================================================

    async def get_all_lines(self) -> List[Line]:
        return await super().get_all_lines(TransportType.FGC)
    
    async def get_stations_by_line_code(self, line_code: str) -> List[Station]:
        return await super().get_stations_by_line_code(TransportType.FGC, line_code)

    async def get_stations_by_name(self, station_name: str) -> List[Station]:
        return await super().get_stations_by_name(station_name, TransportType.FGC)
    
    async def get_station_by_code(self, station_code: str) -> Optional[Station]:
        all_stations = await self.get_stations_by_name("")
        return next((s for s in all_stations if str(s.code) == str(station_code)), None)
    
    async def get_line_by_id(self, line_id: str) -> Optional[Line]:
        lines = await self.get_all_lines()
        return next((l for l in lines if str(l.code) == str(line_id) or str(l.id) == str(line_id)), None)

    # =========================================================================
    # ⚡ MÉTODOS REAL-TIME
    # =========================================================================

    async def get_station_routes(self, station_code: str) -> List[LineRoute]:
        start = time.perf_counter()
        cache_key = f"fgc_station_{station_code}_routes"

        cached = await self.cache_service.get(cache_key)
        if cached: return cached
        
        station = await self.get_station_by_code(station_code)
        if not station: return []

        routes = []

        if station.moute_id:
            try:
                raw_routes = await self.fgc_api_service.get_moute_next_departures(station.moute_id)
                routes = self._map_moute_response(raw_routes)
            except Exception as e:
                logger.warning(f"FGC Moute API failed for {station_code}: {e}")

        if not routes:
            try:
                line_name_clean = station.extra_data.get('line_original_name') or (station.line.name if station.line else "")
                
                raw_routes = await self.fgc_api_service.get_next_departures(station.name, line_name_clean)
                routes = self._map_fallback_response(raw_routes, station)
            except Exception as e:
                logger.error(f"FGC Fallback API failed for {station_code}: {e}")

        if routes:
            await self.cache_service.set(cache_key, routes, ttl=30)
        
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_station_routes({station_code}) -> {len(routes)} routes ({elapsed:.4f}s)")
        return routes

    # --- Helpers de Mapeo (Privados) ---

    def _map_moute_response(self, raw_routes: dict) -> List[LineRoute]:
        routes = []
        for line_name, destinations in raw_routes.items():
            for destination, trips in destinations.items():
                next_trips = [
                    NextTrip(
                        id="", 
                        arrival_time=normalize_to_seconds(int(trip.get("departure_time")))
                    )
                    for trip in trips
                ]
                routes.append(LineRoute(
                    destination=destination,
                    next_trips=next_trips,
                    line_name=line_name,
                    line_code=line_name,
                    line_type=TransportType.FGC,
                    route_id=f"{line_name}-{destination}",
                    color=''
                ))
        return routes

    def _map_fallback_response(self, raw_routes: dict, station: Station) -> List[LineRoute]:
        routes = []
        for direction, trips in raw_routes.items():
            next_trips = [
                NextTrip(
                    id=trip.get("trip_id"), 
                    arrival_time=normalize_to_seconds(trip.get("departure_time"))
                )
                for trip in trips
            ]
            l_name = station.line.name if station.line else "FGC"
            
            routes.append(LineRoute(
                destination=direction,
                next_trips=next_trips,
                line_name=l_name,
                line_type=TransportType.FGC,
                route_id=f"{l_name}-{direction}"
            ))
        return routes