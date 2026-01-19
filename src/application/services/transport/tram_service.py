import asyncio
import time
from typing import List, Optional

# Domain Models
from src.domain.models.common.station import Station
from src.domain.models.common.line import Line
from src.domain.models.common.line_route import LineRoute
from src.domain.models.common.alert import Alert
from src.domain.models.common.connections import Connections
from src.domain.enums.transport_type import TransportType

# Infrastructure & App
from src.infrastructure.external.api.tram_api_service import TramApiService
from src.application.services.user_data_manager import UserDataManager
from src.infrastructure.localization.language_manager import LanguageManager
from src.application.services.cache_service import CacheService
from src.core.logger import logger
from .service_base import ServiceBase

class TramService(ServiceBase):

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

    async def sync_stations(self):
        await super().sync_stations(TransportType.TRAM)

    # --- Implementación de Abstract Methods ---

    async def fetch_lines(self) -> List[Line]:
        return await self.tram_api_service.get_lines()

    async def fetch_stations(self) -> List[Station]:
        api_stations = []
        
        lines = await self.line_repository.get_all(TransportType.TRAM.value)
        if not lines:
            lines = await self.fetch_lines()

        semaphore = asyncio.Semaphore(5)

        async def fetch_line_stops(line):
            line_id = getattr(line, 'original_id', None) or line.code
            async with semaphore:
                return await self.tram_api_service.get_stops_on_line(line_id)

        results = await asyncio.gather(*[fetch_line_stops(line) for line in lines])
        
        for stations in results:
            api_stations.extend(stations)
            
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

    async def get_stations_by_line_code(self, line_code: str) -> List[Station]:          
        return await super().get_stations_by_line_code(TransportType.TRAM, line_code)

    async def get_stations_by_name(self, stop_name: str) -> List[Station]:
        """Búsqueda difusa de paradas por nombre."""
        return await super().get_stations_by_name(stop_name, TransportType.TRAM)

    async def get_stop_by_code(self, stop_code: str) -> Optional[Station]:
        """Busca una parada por código usando la caché masiva."""
        all_stops = await self.get_stations_by_name("")
        return next((s for s in all_stops if str(s.code) == str(stop_code)), None)
    
    async def get_line_by_id(self, line_id: str) -> Optional[Line]:
        lines = await self.get_all_lines()
        return next((l for l in lines if str(l.code) == str(line_id) or l.id == line_id), None)

    # =========================================================================
    # ⚡ MÉTODOS REAL-TIME / ESPECÍFICOS
    # =========================================================================

    async def get_stop_routes(self, stop_code: str) -> List[LineRoute]:
        """
        Obtiene tiempo real del Tram.
        Requiere 'outboundCode' y 'returnCode' que deben estar en extra_data.
        """
        start = time.perf_counter()
        
        stop = await self.get_stop_by_code(stop_code)
        
        if not stop or not stop.extra_data:
            logger.warning(f"Stop {stop_code} not found or missing extra_data for routing")
            return []

        outbound_code = stop.extra_data.get('outboundCode')
        return_code = stop.extra_data.get('returnCode')

        if not outbound_code and not return_code:
             return []

        routes = await self._get_from_cache_or_api(
            cache_key=f"tram_routes_{stop_code}",
            api_call=lambda: self.tram_api_service.get_next_trams_at_stop(outbound_code, return_code),
            cache_ttl=30,
        )
        
        if routes:
            lines = await self.get_all_lines()
            for route in routes:
                matching_line = next((l for l in lines if l.name == route.line_name), None)
                if matching_line:
                    route.line_id = matching_line.id
                    route.line_code = matching_line.code

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stop_routes({stop_code}) -> {len(routes)} routes ({elapsed:.4f}s)")
        return routes

    async def get_tram_stop_connections(self, stop_code: str) -> Connections:
        stop = await self.get_stop_by_code(stop_code)
        if stop and stop.connections:
            return stop.connections
            
        return Connections(lines=[])







'''
import asyncio
import time
from typing import List

from src.domain.models.tram.tram_station import TramStation
from src.domain.models.common.alert import Alert
from src.domain.models.common.line_route import LineRoute
from src.domain.models.common.connections import Connections
from src.domain.models.common.line import Line
from src.domain.enums.transport_type import TransportType
from src.application.services.user_data_manager import UserDataManager
from src.infrastructure.localization.language_manager import LanguageManager
from src.infrastructure.external.api.tram_api_service import TramApiService

from src.core.logger import logger

from src.application.services.cache_service import CacheService
from src.infrastructure.mappers.line_mapper import LineMapper
from .service_base import ServiceBase


class TramService(ServiceBase):
    """
    Service to interact with Tram data via TramApiService, with optional caching.
    """

    def __init__(
        self,
        tram_api_service: TramApiService,
        language_manager: LanguageManager,
        cache_service: CacheService = None,
        user_data_manager: UserDataManager = None
    ):
        start = time.perf_counter()
        super().__init__(cache_service, user_data_manager)
        self.tram_api_service = tram_api_service
        self.language_manager = language_manager
        self.user_data_manager = user_data_manager
        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] TramService initialized (tiempo: {elapsed:.4f} s)")

    async def get_all_lines(self) -> List[Line]:
        return await super().get_all_lines(TransportType.TRAM)
    
    async def fetch_alerts(self) -> List[Alert]:
        api_alerts = await self.tram_api_service.get_global_alerts()
        return [Alert.map_from_tram_alert(a) for a in api_alerts]

    async def fetch_lines(self) -> List[Line]:
        return await self.tram_api_service.get_lines()

    async def sync_lines(self):
        await super().sync_lines(TransportType.TRAM)
    
    async def fetch_stations_by_line(self, line_id: str) -> List[TramStation]:
        return await self.tram_api_service.get_stops_on_line(line_id)

    # === CACHE CALLS ===   
    async def get_all_stops(self) -> List[TramStation]:
        start = time.perf_counter()

        cached_stops = await self.cache_service.get("tram_stops")
        if cached_stops:
            elapsed = (time.perf_counter() - start)
            logger.info(f"[{self.__class__.__name__}] get_all_stops() from cache -> {len(cached_stops)} stops (tiempo: {elapsed:.4f} s)")
            return cached_stops

        lines = await self.get_all_lines()

        stops_lists = await asyncio.gather(
            *[self.get_stops_by_line(line.id) for line in lines]
        )

        all_stops: List[TramStation] = []
        for line, line_stops in zip(lines, stops_lists):
            all_stops.extend(TramStation.update_line_info(s, line) for s in line_stops)
        await self.cache_service.set("tram_stops", all_stops, ttl=3600*24)

        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] get_all_stops() -> {len(all_stops)} stops (tiempo: {elapsed:.4f} s)")
        return all_stops

    async def get_stops_by_line(self, line_id: str) -> List[TramStation]:
        start = time.perf_counter()
        stops = await self._get_from_cache_or_api(
            f"tram_line_{line_id}_stops",
            lambda: self.tram_api_service.get_stops_on_line(line_id),
            cache_ttl=3600*24
        )
        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] get_stops_by_line({line_id}) -> {len(stops)} stops (tiempo: {elapsed:.4f} s)")
        return stops

    async def get_stop_routes(self, stop_code: int) -> List[LineRoute]:
        start = time.perf_counter()
        stop = await self.get_stop_by_code(stop_code)
        routes = await self._get_from_cache_or_api(
            f"tram_routes_{stop_code}",
            lambda: self.tram_api_service.get_next_trams_at_stop(stop.outboundCode, stop.returnCode),
            cache_ttl=30,
        )
        lines = await self.get_all_lines()
        for route in routes:
            if line := next((l for l in lines if l.name == route.line_name), None):
                route.line_id = line.id
                route.line_code = line.code
        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] get_stop_routes({stop_code}) -> {len(routes)} routes (tiempo: {elapsed:.4f} s)")
        return routes

    async def get_tram_stop_connections(self, stop_code) -> Connections:
        start = time.perf_counter()
        connections = await self.cache_service.get(f"tram_stop_connections_{stop_code}")
        if connections:
            elapsed = (time.perf_counter() - start)
            logger.info(f"[{self.__class__.__name__}] get_tram_stop_connections({stop_code}) from cache -> {len(connections)} connections (tiempo: {elapsed:.4f} s)")
            return connections
        
        same_stops = [s for s in await self.get_all_stops() if s.code == stop_code]
        connections = [LineMapper.map_tram_connection(s.line_id, s.line_code, s.line_name, s.line_description, '', '') for s in same_stops]
        await self.cache_service.set(f"tram_stop_connections_{stop_code}", connections, ttl=3600*24)

        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] get_tram_stop_connections({stop_code}) from cache -> {len(connections)} connections (tiempo: {elapsed:.4f} s)")
        return connections

    # === OTHER CALLS ===
    async def get_stops_by_name(self, stop_name):
        start = time.perf_counter()
        stops = await self.get_all_stops()
        if stop_name == '':
            result = stops
        result = self.fuzzy_search(query=stop_name, items=stops, key=lambda s: s.name)
        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] get_stops_by_name({stop_name}) -> {len(result)} stops (tiempo: {elapsed:.4f} s)")
        return result

    async def get_line_by_id(self, line_id) -> Line:
        start = time.perf_counter()
        lines = await self.get_all_lines()
        line = next((l for l in lines if str(l.code) == str(line_id)), None)
        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] get_line_by_id({line_id}) -> {line} (tiempo: {elapsed:.4f} s)")
        return line

    async def get_stop_by_id(self, stop_id) -> TramStation:
        start = time.perf_counter()
        stops = await self.get_all_stops()
        stop = next((s for s in stops if str(s.id) == str(stop_id)), None)
        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] get_stop_by_id({stop_id}) -> {stop} (tiempo: {elapsed:.4f} s)")
        return stop

    async def get_stop_by_code(self, stop_code) -> TramStation:
        start = time.perf_counter()
        stops = await self.get_all_stops()
        stop = next((s for s in stops if str(s.code) == str(stop_code)), None)
        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] get_stop_by_code({stop_code}) -> {stop} (tiempo: {elapsed:.4f} s)")
        return stop
        '''