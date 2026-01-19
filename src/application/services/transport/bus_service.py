
import asyncio
import time
from typing import List, Optional

# Domain Models
from src.domain.models.common.station import Station
from src.domain.models.common.line import Line
from src.domain.models.common.alert import Alert
from src.domain.models.common.connections import Connections
from src.domain.enums.transport_type import TransportType

# Infrastructure & App
from src.infrastructure.external.api.tmb_api_service import TmbApiService
from src.application.services.user_data_manager import UserDataManager
from src.infrastructure.localization.language_manager import LanguageManager
from src.application.services.cache_service import CacheService
from src.core.logger import logger
from .service_base import ServiceBase

class BusService(ServiceBase):
    """
    Servicio para gestionar datos de Bus (TMB).
    Refactorizado para usar Repository, DB y Cache centralizado.
    """

    def __init__(self, 
                 tmb_api_service: TmbApiService,
                 cache_service: CacheService = None,
                 user_data_manager: UserDataManager = None,
                 language_manager: LanguageManager = None):
        super().__init__(cache_service, user_data_manager)
        self.tmb_api_service = tmb_api_service
        self.language_manager = language_manager
        logger.info(f"[{self.__class__.__name__}] BusService initialized")

    # =========================================================================
    # SEEDER
    # =========================================================================

    async def sync_lines(self):
        """Sincroniza las líneas de BUS desde la API a la DB."""
        await super().sync_lines(TransportType.BUS)

    async def sync_stations(self):
        """Sincroniza las paradas de BUS desde la API a la DB."""
        await super().sync_stations(TransportType.BUS)

    # --- Implementación de Abstract Methods ---

    async def fetch_lines(self) -> List[Line]:
        return await self.tmb_api_service.get_bus_lines()
    
    async def fetch_stations(self) -> List[Station]:
        api_stations = []
        lines = lines = await self.line_repository.get_all(TransportType.BUS.value)
        
        semaphore = asyncio.Semaphore(10)

        async def fetch_line_stops(line_code):
            async with semaphore:
                return await self.fetch_stations_by_line(line_code)

        results = await asyncio.gather(*[fetch_line_stops(line.code) for line in lines])
        
        for stops in results:
            api_stations.extend(stops)
            
        return api_stations
    
    async def fetch_stations_by_line(self, line_id: str) -> List[Station]:
        return await self.tmb_api_service.get_bus_line_stops(line_id)

    async def fetch_alerts(self) -> List[Alert]:
        api_alerts = await self.tmb_api_service.get_global_alerts(TransportType.BUS)
        return [Alert.map_from_metro_alert(a) for a in api_alerts]

    async def fetch_station_connections(self, station_code) -> List[any]:
        return await self.tmb_api_service.get_bus_stop_connections(station_code)

    # =========================================================================
    # 🔍 MÉTODOS DE LECTURA (APP)
    # =========================================================================

    async def get_all_lines(self) -> List[Line]:
        return await super().get_all_lines(TransportType.BUS)
    
    async def get_stations_by_line(self, line_code: str) -> List[Station]:
        db_id = line_code
        if not line_code.startswith("bus-"):
            db_id = f"bus-{line_code}"
            
        return await super().get_stations_by_line(TransportType.BUS, line_id=db_id)

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
        
        # Lógica de rango numérico "1-100" (Buses convencionales)
        if "-" in bus_category and bus_category.replace("-", "").isdigit():
            start_cat, end_cat = map(int, bus_category.split("-"))
            for line in lines:
                # Verificamos si el nombre es numérico y está en rango
                if line.name.isdigit():
                    if start_cat <= int(line.name) <= end_cat:
                        result.append(line)
        else:
            # Lógica por categoría textual (H, V, D, NitBus...)
            # Asumimos que TmbApiService o LineMapper guardaron la categoría en line.category
            # O filtramos por prefijo del nombre
            for line in lines:
                # Opción A: Usar campo category si está bien poblado
                if line.category and line.category.upper() == bus_category.upper():
                     result.append(line)
                # Opción B: Fallback a prefijos de nombre (H12, V15, N0)
                elif line.name.upper().startswith(bus_category.upper()):
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
            cache_ttl=10
        )
        
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stop_routes({stop_code}) -> {len(data) if isinstance(data, list) else 'Data'} ({elapsed:.4f} s)")
        return data

    async def get_stop_connections(self, stop_code: str) -> Connections:
        from src.domain.models.common.connections import Connections
        
        data = await self._get_from_cache_or_api(
            cache_key=f"bus_stop_{stop_code}_connections",
            api_call=lambda: self.tmb_api_service.get_bus_stop_connections(stop_code),
            cache_ttl=3600*24
        )
        
        if isinstance(data, list):
            return Connections(lines=data)
        return data












    '''

    def __init__(self, tmb_api_service: TmbApiService,
                 cache_service: CacheService = None,
                 user_data_manager: UserDataManager = None,
                 language_manager: LanguageManager = None):
        super().__init__(cache_service, user_data_manager)
        self.tmb_api_service = tmb_api_service
        self.user_data_manager = user_data_manager
        self.language_manager = language_manager
        logger.info(f"[{self.__class__.__name__}] BusService initialized")


    async def get_all_lines(self) -> List[Line]:
        return await super().get_all_lines(TransportType.BUS)
    
    async def get_stations_by_line(self, line_code) -> List[Station]:
        return await super().get_stations_by_line(TransportType.BUS, line_id=line_code)
    
    async def fetch_alerts(self) -> List[Alert]:
        api_alerts = await self.tmb_api_service.get_global_alerts(TransportType.BUS)
        return [Alert.map_from_metro_alert(a) for a in api_alerts]

    async def fetch_lines(self) -> List[Line]:
        return await self.tmb_api_service.get_bus_lines()
    
    async def fetch_stations(self) -> List[Station]:
        api_stations = []
        for line in await self.get_all_lines():
            line_stations = await self.tmb_api_service.get_bus_line_stops(line.code)
            api_stations.extend(line_stations)
        return api_stations
    
    async def fetch_station_connections(self, station_id) -> Connections:
        return await self.tmb_api_service.get_bus_stop_connections(station_id)

    async def sync_lines(self):
        await super().sync_lines(TransportType.BUS)

    async def sync_stations(self):
        await super().sync_stations(TransportType.BUS)









    async def get_all_stops(self) -> List[BusStop]:
        start = time.perf_counter()
        static_stops = await self.cache_service.get("bus_stops_static")
        alerts_by_stop = await self.cache_service.get("bus_stops_alerts")

        if not static_stops and not alerts_by_stop:
            static_stops, alerts_by_stop = await asyncio.gather(
                self._build_and_cache_static_stops(),
                self._build_and_cache_stop_alerts()
            )
        elif not static_stops:
            static_stops = await self._build_and_cache_static_stops()
        elif not alerts_by_stop:
            alerts_by_stop = await self._build_and_cache_stop_alerts()

        for stop in static_stops:
            stop_alerts = alerts_by_stop.get(stop.code, [])
            stop.has_alerts = any(stop_alerts)
            stop.alerts = stop_alerts if any(stop_alerts) else []

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_all_stops() -> {len(static_stops)} stops ({elapsed:.4f} s)")
        return static_stops

    async def get_stops_by_line(self, line_id) -> List[BusStop]:
        start = time.perf_counter()
        cache_key = f"bus_line_{line_id}_stops"
        
        cached_stations = await self._get_from_cache_or_data(cache_key, None, cache_ttl=3600*24)
        if cached_stations:
            elapsed = time.perf_counter() - start
            logger.info(f"[{self.__class__.__name__}] get_stops_by_line({line_id}) -> cached ({elapsed:.4f} s)")
            return cached_stations

        line_task = self.get_line_by_id(line_id)
        stops_task = self.tmb_api_service.get_bus_line_stops(line_id)        
        line, api_stops = await asyncio.gather(line_task, stops_task)

        line_stops = [BusStop.update_bus_stop_with_line_info(api_stop, line) for api_stop in api_stops]
        connections_tasks = [self.get_stop_connections(stop.code) for stop in line_stops]        
        connections_results = await asyncio.gather(*connections_tasks)

        for stop, connections in zip(line_stops, connections_results):
            stop.connections = connections

        result = await self._get_from_cache_or_data(cache_key, line_stops, cache_ttl=3600*24)

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stops_by_line({line_id}) -> {len(result)} stops ({elapsed:.4f} s)")
        return result

    async def get_stop_routes(self, stop_code: str) -> str:
        start = time.perf_counter()
        routes = await self._get_from_cache_or_api(
            f"bus_stop_{stop_code}_routes",
            lambda: self.tmb_api_service.get_next_bus_at_stop(stop_code),
            cache_ttl=10
        )

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stop_routes({stop_code}) -> {len(routes)} routes ({elapsed:.4f} s)")
        return routes
    
    async def get_stop_connections(self, stop_code) -> Connections:
        start = time.perf_counter()
        data = await self._get_from_cache_or_api(
            f"bus_stop_{stop_code}_connections",
            lambda: self.tmb_api_service.get_bus_stop_connections(stop_code),
            cache_ttl=3600*24
        )
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stop_connections({stop_code}) -> {len(data)} connections ({elapsed:.4f} s)")
        return data

    # === OTHER CALLS ===
    async def get_stops_by_name(self, stop_name) -> List[Line]:
        start = time.perf_counter()
        stops = await self.get_all_stops()

        if stop_name != '':
            result = self.fuzzy_search(
                query=stop_name,
                items=stops,
                key=lambda stop: stop.name
            )
        else:
            result = stops
            
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stops_by_name({stop_name}) -> {len(result)} matches ({elapsed:.4f} s)")
        return result

    async def get_line_by_id(self, line_id) -> Line:
        start = time.perf_counter()
        lines = await self.get_all_lines()
        line = next((l for l in lines if str(l.code) == str(line_id)), None)
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_line_by_id({line_id}) -> {line} ({elapsed:.4f} s)")
        return line

    async def get_lines_by_category(self, bus_category: str):
        start = time.perf_counter()
        lines = await self.get_all_lines()
        if "-" in bus_category:
            start_cat, end_cat = bus_category.split("-")
            result = [
                line for line in lines
                if int(start_cat) <= int(line.code) <= int(end_cat)
                and line.name.isdigit()
            ]
        else:
            result = [
                line for line in lines
                if bus_category == line.category
            ]
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_lines_by_category({bus_category}) -> {len(result)} lines ({elapsed:.4f} s)")
        return result

    async def get_stop_by_code(self, stop_code) -> BusStop:
        start = time.perf_counter()
        stops = await self.get_all_stops()
        filtered_stops = [
            stop for stop in stops
            if int(stop_code) == int(stop.code)
        ]
        result = next((bs for bs in filtered_stops if bs.has_alerts),
                      filtered_stops[0] if filtered_stops else None)
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stop_by_code({stop_code}) -> {result} ({elapsed:.4f} s)")
        return result

    async def _build_and_cache_static_stops(self) -> List[BusStop]:
        start = time.perf_counter()
        lines = await self.get_all_lines()

        semaphore_lines = asyncio.Semaphore(5)
        semaphore_connections = asyncio.Semaphore(10)

        stops: List[BusStop] = []

        async def process_stop(api_stop: BusStop, line: Line):
            async with semaphore_connections:
                stop = BusStop.update_bus_stop_with_line_info(api_stop, line)
                stops.append(stop)

        async def process_line(line: Line):
            async with semaphore_lines:
                api_stops = await self.tmb_api_service.get_bus_line_stops(line.code)
            await asyncio.gather(*(process_stop(stop, line) for stop in api_stops))

        await asyncio.gather(*(process_line(line) for line in lines))

        await self.cache_service.set("bus_stops_static", stops, ttl=3600 * 24)
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] _build_and_cache_static_stops() -> {len(stops)} stops ({elapsed:.4f} s)")
        return stops

    async def _build_and_cache_stop_alerts(self) -> dict:
        start = time.perf_counter()
        alerts_by_stop = defaultdict(list)
        lines = await self.get_all_lines()
        alert_lines = [line for line in lines if line.has_alerts]

        semaphore = asyncio.Semaphore(10)

        async def process_line(line: Line):
            async with semaphore:
                stops = await self.get_stops_by_line(line.code)
            return [(stop.code, stop.alerts) for stop in stops]

        results = await asyncio.gather(*[process_line(line) for line in alert_lines])
        for stop_list in results:
            for code, alerts in stop_list:
                alerts_by_stop[code].extend(alerts)

        alerts_dict = dict(alerts_by_stop)
        await self.cache_service.set("bus_stops_alerts", alerts_dict, ttl=3600)
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] _build_and_cache_stop_alerts() -> {len(alerts_dict)} stops with alerts ({elapsed:.4f} s)")
        return alerts_dict
        '''
