import asyncio
import time
from typing import List, Optional

# Domain Models
from src.domain.models.common.station import Station
from src.domain.models.common.line import Line
from src.domain.models.common.line_route import LineRoute
from src.domain.models.common.alert import Alert
from src.domain.enums.transport_type import TransportType

# Infrastructure & App
from src.infrastructure.external.api.tram_api_service import TramApiService
from src.application.services.user_data_manager import UserDataManager
from src.infrastructure.localization.language_manager import LanguageManager
from src.application.services.cache_service import CacheService
from src.core.logger import logger
from .service_base import ServiceBase

class TramService(ServiceBase):
    """
    Servicio para gestionar datos de TRAM.
    Optimizado para llamadas paralelas controladas.
    """

    def __init__(
        self,
        tram_api_service: TramApiService,
        language_manager: LanguageManager,
        cache_service: CacheService = None,
        user_data_manager: UserDataManager = None
    ):
        super().__init__(cache_service, user_data_manager)
        self.tram_api_service = tram_api_service
        self.language_manager = language_manager
        logger.info(f"[{self.__class__.__name__}] TramService initialized")

    # =========================================================================
    # 🔄 MÉTODOS DE SINCRONIZACIÓN (SEEDER)
    # =========================================================================

    async def sync_lines(self):
        await super().sync_lines(TransportType.TRAM)

    async def sync_stations(self, valid_lines_filter):
        await super().sync_stations(TransportType.TRAM, valid_lines_filter)

    async def fetch_lines(self) -> List[Line]:
        return await self.tram_api_service.get_lines()

    async def fetch_stations(self) -> List[Station]:
        lines = await self.line_repository.get_all(TransportType.TRAM.value)
        if not lines:
            lines = await self.fetch_lines()

        semaphore = asyncio.Semaphore(5)

        async def fetch_line_stops(line):
            line_id = getattr(line, 'original_id', None) or line.code
            async with semaphore:
                try:
                    return await self.tram_api_service.get_stops_on_line(line_id)
                except Exception as e:
                    logger.error(f"Error fetching TRAM line {line_id}: {e}")
                    return []

        results = await asyncio.gather(*[fetch_line_stops(line) for line in lines])
        
        api_stations = [s for sublist in results for s in sublist]
            
        return api_stations
    
    async def fetch_stations_by_line(self, line_id: str) -> List[Station]:
        return await self.tram_api_service.get_stops_on_line(line_id)

    async def fetch_alerts(self) -> List[Alert]:
        api_alerts = await self.tram_api_service.get_global_alerts()
        return [Alert.map_from_tram_alert(a) for a in api_alerts]
    
    # =========================================================================
    # 🔍 MÉTODOS DE LECTURA (APP)
    # =========================================================================

    async def get_all_lines(self) -> List[Line]:
        return await super().get_all_lines(TransportType.TRAM)

    async def get_stations_by_line_id(self, line_id: str) -> List[Station]:          
        return await super().get_stations_by_line_id(TransportType.TRAM, line_id)

    async def get_stations_by_name(self, stop_name: str) -> List[Station]:
        return await super().get_stations_by_name(stop_name, TransportType.TRAM)

    async def get_stop_by_code(self, stop_code: str) -> Optional[Station]:
        return await super().get_station_by_code(stop_code, TransportType.TRAM)
    
    async def get_line_by_id(self, line_id: str) -> Optional[Line]:
        lines = await self.get_all_lines()
        return next((l for l in lines if str(l.code) == str(line_id) or l.id == line_id), None)

    # =========================================================================
    # ⚡ MÉTODOS REAL-TIME / ESPECÍFICOS
    # =========================================================================

    async def get_stop_routes(self, physical_station_id: str, line_id: str) -> List[LineRoute]:
        start = time.perf_counter()
        
        await self._ensure_lines_cache()        
        line_metadata = self._lines_metadata_cache.get(line_id)
        if not line_metadata:
            logger.warning(f"⚠️ Metadata not found for line_id: {line_id}")
            return []
        
        route_stop = await self.stations_repository.get_stop_by_physical_and_line_id(
            physical_station_id, 
            line_id
        )

        if not route_stop:
            logger.warning(f"⚠️ Tram Stop not found for {physical_station_id} + {line_id}")
            return []

        station = route_stop.station
        extra = station.extra_data or {}
        outbound = extra.get('outbound_code')
        inbound = extra.get('return_code')

        if not outbound and not inbound:
            return []

        raw_cache_key = f"tram_full_response_{outbound}_{inbound}"

        all_routes = await self._get_from_cache_or_api(
            cache_key=raw_cache_key,
            api_call=lambda: self.tram_api_service.get_next_trams_at_stop(outbound, inbound),
            cache_ttl=30,
        )

        if not all_routes:
            return []

        target_line_name = line_metadata.name.upper()
        filtered_routes = []

        for route in all_routes:
            if route.line_name.upper() == target_line_name:
                route.line_id = line_id
                route.color = line_metadata.color or "008E78"
                
                filtered_routes.append(route)

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] RT {line_id} @ {physical_station_id} -> {len(filtered_routes)} routes (taken from pool of {len(all_routes)}) ({elapsed:.4f}s)")
        
        return filtered_routes