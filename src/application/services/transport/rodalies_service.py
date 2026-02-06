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
from src.infrastructure.external.api.rodalies_api_service import RodaliesApiService
from src.application.services.user_data_manager import UserDataManager
from src.infrastructure.localization.language_manager import LanguageManager
from src.application.services.cache_service import CacheService
from src.core.logger import logger
from .service_base import ServiceBase

class RodaliesService(ServiceBase):
    """
    Servicio para gestionar datos de Rodalies.
    Optimizado para llamadas paralelas controladas (API Renfe es sensible).
    """

    def __init__(self, 
                 rodalies_api_service: RodaliesApiService, 
                 language_manager: LanguageManager, 
                 cache_service: CacheService = None, 
                 user_data_manager: UserDataManager = None):
        super().__init__(cache_service, user_data_manager)
        self.rodalies_api_service = rodalies_api_service
        self.language_manager = language_manager
        logger.info(f"[{self.__class__.__name__}] RodaliesService initialized")

    # =========================================================================
    # 🔄 SYNC & FETCH IMPLEMENTATIONS
    # =========================================================================

    async def sync_lines(self):
        await super().sync_lines(TransportType.RODALIES)

    async def sync_stations(self, valid_lines_filter):
        await super().sync_stations(TransportType.RODALIES, valid_lines_filter)

    async def sync_alerts(self):
        await super().sync_alerts(TransportType.RODALIES)

    async def fetch_lines(self) -> List[Line]:
        return await self.rodalies_api_service.get_lines()

    async def fetch_stations(self) -> List[Station]:
        lines = await self.line_repository.get_all(TransportType.RODALIES.value)
        if not lines:
            lines = await self.fetch_lines()
        
        semaphore = asyncio.Semaphore(5)

        async def fetch_safe(line: Line):
            async with semaphore:
                try:
                    identifier = line.original_id if line.original_id else line.code
                    return await self.fetch_stations_by_line(identifier)
                except Exception as e:
                    logger.error(f"Error fetching Rodalies line {line.code}: {e}")
                    return []

        results = await asyncio.gather(*[fetch_safe(line) for line in lines])
        
        api_stations = [s for sublist in results for s in sublist]
            
        return api_stations
    
    async def fetch_stations_by_line(self, line_id: str) -> List[Station]:
        return await self.rodalies_api_service.get_stations_by_line_id(line_id)

    async def fetch_alerts(self) -> List[Alert]:
        api_alerts = await self.rodalies_api_service.get_global_alerts()
        return [Alert.map_from_rodalies_alert(a) for a in api_alerts]

    # =========================================================================
    # 🔍 MÉTODOS DE LECTURA (APP)
    # =========================================================================

    async def get_all_lines(self) -> List[Line]:
        return await super().get_all_lines(TransportType.RODALIES)
    
    async def get_stations_by_line_id(self, line_id: str) -> List[Station]:           
        return await super().get_stations_by_line_id(TransportType.RODALIES, line_id)

    async def get_stations_by_name(self, station_name: str) -> List[Station]:
        return await super().get_stations_by_name(station_name, TransportType.RODALIES)

    async def get_station_by_code(self, station_code: str) -> Optional[Station]:
        return await super().get_station_by_code(station_code, TransportType.RODALIES)
    
    async def get_line_by_id(self, line_id: str) -> Optional[Line]:
        return await super().get_line_by_id(TransportType.RODALIES, line_id)

    # =========================================================================
    # ⚡ MÉTODOS REAL-TIME
    # =========================================================================

    async def get_station_routes(self, physical_station_id: str, line_id: str) -> List[LineRoute]:
        start = time.perf_counter()
        
        await self._ensure_lines_cache()        
        line_metadata = self._lines_metadata_cache.get(line_id)
        if not line_metadata:
            logger.warning(f"⚠️ Metadata not found for line_id: {line_id}")
            return []

        route_stop = await self.stations_repository.get_stop_by_physical_and_line_id(physical_station_id, line_id)
        if not route_stop:
            logger.warning(f"⚠️ No se encontró RouteStop para {physical_station_id} + {line_id}")
            return []
        
        external_code = route_stop.station_external_code
        cache_key = f"rodalies_full_{external_code}"

        all_routes = await self._get_from_cache_or_api(
            cache_key=cache_key,
            api_call=lambda: self.rodalies_api_service.get_next_trains_at_station(external_code),
            cache_ttl=30
        )

        if not all_routes:
            return []

        target_line_name = line_metadata.name.upper() # "R4"
        filtered_routes = []

        for route in all_routes:
            if route.line_name.upper() == target_line_name:
                route.line_id = line_id 
                route.color = line_metadata.color 
                
                filtered_routes.append(route)        

        unique_routes = list({r.route_id: r for r in filtered_routes}.values())

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] RT {line_id} @ {external_code} -> {len(unique_routes)} routes (from pool of {len(all_routes)}) ({elapsed:.4f}s)")
        
        return unique_routes