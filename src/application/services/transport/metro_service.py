import asyncio
import time
from typing import List, Optional

from src.domain.models.metro.metro_access import MetroAccess
from src.domain.models.common.alert import Alert
from src.domain.models.common.station import Station
from src.domain.models.common.line import Line
from src.domain.models.common.line_route import LineRoute
from src.domain.enums.transport_type import TransportType
from src.application.services.user_data_manager import UserDataManager
from src.infrastructure.localization.language_manager import LanguageManager
from src.infrastructure.external.api.tmb_api_service import TmbApiService
from src.core.logger import logger
from src.application.services.cache_service import CacheService
from .service_base import ServiceBase

class MetroService(ServiceBase):
    """
    Service to interact with Metro data via TmbApiService, with optional caching.
    """    

    def __init__(self, tmb_api_service: TmbApiService, language_manager: LanguageManager,
                 cache_service: CacheService = None, user_data_manager: UserDataManager = None):
        super().__init__(cache_service, user_data_manager)
        self.tmb_api_service = tmb_api_service
        self.language_manager = language_manager
        
        logger.info(f"[{self.__class__.__name__}] MetroService initialized")

    # =========================================================================
    # 🔄 SYNC & FETCH IMPLEMENTATIONS
    # =========================================================================

    async def sync_lines(self):
        await super().sync_lines(TransportType.METRO)

    async def sync_stations(self, valid_lines_filter):
        await super().sync_stations(TransportType.METRO, valid_lines_filter)

    async def fetch_lines(self) -> List[Line]:
        return await self.tmb_api_service.get_metro_lines()
    
    async def fetch_stations(self) -> List[Station]:
        lines = await self.line_repository.get_all(TransportType.METRO.value)
        if not lines:
            return []

        tasks = [self.fetch_stations_by_line(line.code) for line in lines]
        results = await asyncio.gather(*tasks)

        flat_stations = [station for sublist in results for station in sublist]
        
        return flat_stations

    async def fetch_stations_by_line(self, line_id: str) -> List[Station]:
        return await self.tmb_api_service.get_stations_by_metro_line(line_id)

    async def fetch_alerts(self) -> List[Alert]:
        api_alerts = await self.tmb_api_service.get_global_alerts(TransportType.METRO)
        return [Alert.map_from_metro_alert(a) for a in api_alerts]

    # =========================================================================
    # 🔍 READ METHODS (Overrides & Specifics)
    # =========================================================================

    async def get_all_lines(self) -> List[Line]:
        return await super().get_all_lines(TransportType.METRO)

    async def get_stations_by_line_code(self, line_code: str) -> List[Station]:            
        return await super().get_stations_by_line_code(TransportType.METRO, line_code)

    async def get_stations_by_name(self, station_name: str) -> List[Station]:
        return await super().get_stations_by_name(station_name, TransportType.METRO)

    async def get_station_by_code(self, station_code: str) -> Optional[Station]:
        start = time.perf_counter()
        
        all_stations = await self.get_stations_by_name("")        
        station = next((s for s in all_stations if str(s.code) == str(station_code)), None)
        
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_station_by_code({station_code}) found: {station is not None} ({elapsed:.4f}s)")
        return station

    async def get_line_by_code(self, line_code: str) -> Optional[Line]:
        lines = await self.get_all_lines()
        return next((l for l in lines if str(l.code) == str(line_code)), None)

    async def get_line_by_name(self, line_name: str) -> Optional[Line]:
        lines = await self.get_all_lines()
        return next((l for l in lines if str(l.name) == str(line_name)), None)
    
    # =========================================================================
    # ⚡ REAL TIME & SPECIFIC FEATURES
    # =========================================================================

    async def get_station_routes(self, station_code: str) -> List[LineRoute]:
        start = time.perf_counter()
        cache_key = f"metro_station_{station_code}_routes"

        cached_routes = await self.cache_service.get(cache_key)
        if cached_routes:
             return cached_routes

        routes = await self.tmb_api_service.get_next_metro_at_station(station_code)
        
        routes = list({r.route_id: r for r in routes}.values())

        if not any(r.next_trips for r in routes):
            logger.debug(f"Sin tiempo real para {station_code}, buscando horarios...")
            routes = await self.tmb_api_service.get_next_scheduled_metro_at_station(station_code)
        
        if routes:
            await self.cache_service.set(cache_key, routes, ttl=15)

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_station_routes({station_code}) -> {len(routes)} routes ({elapsed:.4f}s)")
        return routes

    async def get_station_accesses(self, group_code_id: str) -> List[MetroAccess]:
        start = time.perf_counter()
        
        data = await self._get_from_cache_or_api(
            cache_key=f"metro_station_{group_code_id}_accesses",
            api_call=lambda: self.tmb_api_service.get_metro_station_accesses(group_code_id),
            cache_ttl=86400 * 30
        )
        
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_station_accesses({group_code_id}) -> {len(data)} accesses ({elapsed:.4f}s)")
        return data