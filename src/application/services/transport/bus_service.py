import asyncio
import time
from typing import List, Optional

from src.domain.models.common.station import Station
from src.domain.models.common.line import Line
from src.domain.models.common.alert import Alert
from src.domain.enums.transport_type import TransportType

from src.infrastructure.external.api.tmb_api_service import TmbApiService
from src.application.services.user_data_manager import UserDataManager
from src.infrastructure.localization.language_manager import LanguageManager
from src.application.services.cache_service import CacheService
from src.core.logger import logger
from .service_base import ServiceBase

class BusService(ServiceBase):
    """
    Servicio para gestionar datos de Bus (TMB).
    Optimizado con procesamiento paralelo limitado (Semáforo) para evitar rate-limits.
    """

    def __init__(self, tmb_api_service: TmbApiService,
                 cache_service: CacheService = None,
                 user_data_manager: UserDataManager = None,
                 language_manager: LanguageManager = None):
        super().__init__(cache_service, user_data_manager)
        self.tmb_api_service = tmb_api_service
        self.language_manager = language_manager
        logger.info(f"[{self.__class__.__name__}] BusService initialized")

    # =========================================================================
    # 🔄 SYNC & FETCH IMPLEMENTATIONS
    # =========================================================================

    async def sync_lines(self):
        await super().sync_lines(TransportType.BUS)

    async def sync_stations(self):
        await super().sync_stations(TransportType.BUS)

    async def fetch_lines(self) -> List[Line]:
        return await self.tmb_api_service.get_bus_lines()
    
    async def fetch_stations(self) -> List[Station]:
        lines = await self.line_repository.get_all(TransportType.BUS.value)
        if not lines:
            lines = await self.fetch_lines()
        
        semaphore = asyncio.Semaphore(10)

        async def fetch_line_stops_safe(line_code):
            async with semaphore:
                try:
                    return await self.fetch_stations_by_line(line_code)
                except Exception as e:
                    logger.error(f"Error fetching bus line {line_code}: {e}")
                    return []

        tasks = [fetch_line_stops_safe(line.code) for line in lines]
        results = await asyncio.gather(*tasks)
        
        api_stations = [stop for sublist in results for stop in sublist]
            
        return api_stations
    
    async def fetch_stations_by_line(self, line_id: str) -> List[Station]:
        return await self.tmb_api_service.get_bus_line_stops(line_id)

    async def fetch_alerts(self) -> List[Alert]:
        api_alerts = await self.tmb_api_service.get_global_alerts(TransportType.BUS)
        return [Alert.map_from_metro_alert(a) for a in api_alerts]

    # =========================================================================
    # 🔍 MÉTODOS DE LECTURA (APP)
    # =========================================================================

    async def get_all_lines(self) -> List[Line]:
        return await super().get_all_lines(TransportType.BUS)
    
    async def get_stops_by_line_code(self, line_code: str) -> List[Station]:       
        return await super().get_stations_by_line_code(TransportType.BUS, line_code)

    async def get_stops_by_name(self, stop_name: str) -> List[Station]:
        return await super().get_stations_by_name(stop_name, TransportType.BUS)
    
    async def get_stop_by_code(self, stop_code: str) -> Optional[Station]:
        all_stops = await self.get_stops_by_name("")        
        return next((s for s in all_stops if str(s.code) == str(stop_code)), None)

    async def get_line_by_id(self, line_id: str) -> Optional[Line]:
        lines = await self.get_all_lines()
        return next((l for l in lines if str(l.code) == str(line_id)), None)

    async def get_lines_by_category(self, bus_category: str) -> List[Line]:
        start = time.perf_counter()
        
        lines = await self.get_all_lines()
        result = []
        bus_cat_upper = bus_category.upper()
        
        if "-" in bus_category and bus_category.replace("-", "").isdigit():
            try:
                start_cat, end_cat = map(int, bus_category.split("-"))
                for line in lines:
                    if line.name.isdigit() and start_cat <= int(line.name) <= end_cat:
                        result.append(line)
            except ValueError:
                pass

        else:
            for line in lines:
                if line.category and line.category.upper() == bus_cat_upper:
                     result.append(line)
                
                elif line.name.upper().startswith(bus_cat_upper):
                    result.append(line)

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_lines_by_category({bus_category}) -> {len(result)} lines ({elapsed:.4f} s)")
        return result

    # =========================================================================
    # ⚡ MÉTODOS REAL-TIME (iBus)
    # =========================================================================

    async def get_stop_routes(self, stop_code: str) -> any:
        start = time.perf_counter()
        
        data = await self._get_from_cache_or_api(
            cache_key=f"bus_stop_{stop_code}_routes",
            api_call=lambda: self.tmb_api_service.get_next_bus_at_stop(stop_code),
            cache_ttl=15
        )
        
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stop_routes({stop_code}) -> iBus Data ({elapsed:.4f} s)")
        return data