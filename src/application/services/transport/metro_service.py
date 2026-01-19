import asyncio
from collections import defaultdict
from time import time
from typing import List, Optional
import time

from src.domain.models.metro.metro_station import MetroStation
from src.domain.models.metro.metro_access import MetroAccess
from src.domain.models.common.alert import Alert
from src.domain.models.common.station import Station
from src.domain.models.common.line import Line
from src.domain.models.common.line_route import LineRoute
from src.domain.models.common.connections import Connections
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
        self.user_data_manager = user_data_manager
        logger.info(f"[{self.__class__.__name__}] MetroService initialized")

        
    # SEEDERS
    async def sync_lines(self):
        await super().sync_lines(TransportType.METRO)

    async def sync_stations(self):
        await super().sync_stations(TransportType.METRO)


    # ABSTRACT METHODS IMPLEMENTATION
    async def fetch_lines(self) -> List[Line]:
        return await self.tmb_api_service.get_metro_lines()
    
    async def fetch_stations(self) -> List[Station]:
        api_stations = []
        lines = await self.line_repository.get_all(TransportType.METRO.value)
        
        for line in lines:
            line_stations = await self.fetch_stations_by_line(line.code)
            api_stations.extend(line_stations)
            
        return api_stations
    
    async def fetch_alerts(self) -> List[Alert]:
        api_alerts = await self.tmb_api_service.get_global_alerts(TransportType.METRO)
        return [Alert.map_from_metro_alert(a) for a in api_alerts]

    async def fetch_station_connections(self, station_code) -> List[any]:
        return await self.tmb_api_service.get_metro_station_connections(station_code)
    
    async def fetch_stations_by_line(self, line_id: str) -> List[Station]:
        return await self.tmb_api_service.get_stations_by_metro_line(line_id)
    

    # READ METHODS WITH CACHE
    async def get_all_lines(self) -> List[Line]:
        return await super().get_all_lines(TransportType.METRO)

    async def get_stations_by_line(self, line_code: str) -> List[Station]:            
        return await super().get_stations_by_line(TransportType.METRO, line_id=line_code)

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
            await self.cache_service.set(cache_key, routes, ttl=10)

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_station_routes({station_code}) -> {len(routes)} routes ({elapsed:.4f}s)")
        return routes

    async def get_station_accesses(self, group_code_id: str) -> List[MetroAccess]:
        start = time.perf_counter()
        
        data = await self._get_from_cache_or_api(
            cache_key=f"metro_station_{group_code_id}_accesses",
            api_call=lambda: self.tmb_api_service.get_metro_station_accesses(group_code_id),
            cache_ttl=3600*24
        )
        
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_station_accesses({group_code_id}) -> {len(data)} accesses ({elapsed:.4f}s)")
        return data

    async def get_station_connections(self, station_code: str) -> Connections:
        from src.domain.models.common.connections import Connections

        station = await self.get_station_by_code(station_code)

        if station and station.connections:
            return station.connections
        else:
            return Connections(lines=[])
    


    '''

    async def get_all_lines(self) -> List[Line]:
        return await super().get_all_lines(TransportType.METRO)

    async def get_stations_by_line(self, line_code) -> List[Station]:
        return await super().get_stations_by_line(TransportType.METRO, line_id=line_code)
    
    async def fetch_alerts(self) -> List[Alert]:
        api_alerts = await self.tmb_api_service.get_global_alerts(TransportType.METRO)
        return [Alert.map_from_metro_alert(a) for a in api_alerts]

    async def fetch_lines(self) -> List[Line]:
        return await self.tmb_api_service.get_metro_lines()
    
    async def fetch_stations(self) -> List[Station]:
        api_stations = []
        for line in await self.get_all_lines():
            line_stations = await self.tmb_api_service.get_stations_by_metro_line(line.code)
            api_stations.extend(line_stations)
        return api_stations
    
    async def fetch_station_connections(self, station_id) -> Connections:
        return await self.tmb_api_service.get_metro_station_connections(station_id)


    async def get_stations_by_name(self, station_name: str) -> List[Station]:
        start = time.perf_counter()
        
        cache_key = f"all_stations_{TransportType.METRO.value}"

        async def fetch_and_map_all_stations():
            models = await self.stations_repository.get_by_transport_type(TransportType.METRO.value)
            
            mapped_stations = []
            from src.domain.models.common.connections import Connections

            for model in models:
                st = Station.model_validate(model)
                
                if model.connections_data and not st.connections:
                    try:
                        st.connections = Connections.model_validate(model.connections_data)
                    except Exception:
                        pass
                
                mapped_stations.append(st)
            return mapped_stations

        all_stations = await self._get_from_cache_or_api(
            cache_key=cache_key,
            api_call=fetch_and_map_all_stations,
            cache_ttl=86400
        )

        if not station_name:
            result = all_stations
        else:
            result = self.fuzzy_search(
                query=station_name,
                items=all_stations,
                key=lambda s: s.name,
                threshold=75 
            )

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stations_by_name('{station_name}') -> {len(result)} matches ({elapsed:.4f}s)")
        
        return result




    
    
    async def get_station_connections(self, station_code) -> Connections:
        start = time.perf_counter()
        data = await self._get_from_cache_or_api(
            f"metro_station_{station_code}_connections",
            lambda: self.tmb_api_service.get_metro_station_connections(station_code),
            cache_ttl=3600*24
        )
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_station_connections({station_code}) -> {len(data)} connections ({elapsed:.4f} s)")
        return data

    async def get_station_accesses(self, group_code_id) -> List[MetroAccess]:
        start = time.perf_counter()
        data = await self._get_from_cache_or_api(
            f"metro_station_{group_code_id}_accesses",
            lambda: self.tmb_api_service.get_metro_station_accesses(group_code_id),
            cache_ttl=3600*24
        )
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_station_accesses({group_code_id}) -> {len(data)} accesses ({elapsed:.4f} s)")
        return data

    async def get_station_routes(self, station_code) -> List[LineRoute]:
        start = time.perf_counter()

        routes = await self._get_from_cache_or_data(
            f"metro_station_{station_code}_routes",
            None,
            cache_ttl=10
        )

        if not routes:
            routes = await self.tmb_api_service.get_next_metro_at_station(station_code)
            routes = list({r.route_id: r for r in routes}.values())
            if not any(r.next_trips for r in routes):
                routes = await self.tmb_api_service.get_next_scheduled_metro_at_station(station_code)
            
        routes = await self._get_from_cache_or_data(
            f"metro_station_{station_code}_routes",
            routes,
            cache_ttl=10
        )
        
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_station_routes({station_code}) -> {len(routes)} routes ({elapsed:.4f} s)")
        return routes

    

    async def get_station_by_code(self, station_code) -> MetroStation:
        start = time.perf_counter()
        stations = await self.get_all_stations()
        filtered_stations = [station for station in stations if int(station_code) == int(station.code)]
        station = filtered_stations[0] if filtered_stations else None
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_station_by_id({station_code}) -> {station} ({elapsed:.4f} s)")
        return station

    async def get_line_by_code(self, line_code) -> Line:
        start = time.perf_counter()
        lines = await self.get_all_lines()
        line = next((l for l in lines if str(l.code) == str(line_code)), None)
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_line_by_id({line_code}) -> {line} ({elapsed:.4f} s)")
        return line

    async def get_line_by_name(self, line_name):
        start = time.perf_counter()
        lines = await self.get_all_lines()
        line = next((l for l in lines if str(l.name) == str(line_name)), None)
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_line_by_name({line_name}) -> {line} ({elapsed:.4f} s)")
        return line

    async def _build_and_cache_static_stations(self) -> List[MetroStation]:
        start = time.perf_counter()
        lines = await self.get_all_lines()
        stations = []

        semaphore_lines = asyncio.Semaphore(5)
        semaphore_connections = asyncio.Semaphore(10)

        async def process_station(api_station: Station, line: Line):
            async with semaphore_connections:
                return MetroStation.update_metro_station_with_line_info(api_station, line)

        async def process_line(line):
            async with semaphore_lines:
                line_stations = await self.tmb_api_service.get_stations_by_metro_line(line.code)
            processed_stations = await asyncio.gather(*[process_station(s, line) for s in line_stations])
            return processed_stations

        results = await asyncio.gather(*[process_line(line) for line in lines])

        for line_stations in results:
            stations.extend(line_stations)

        await self.cache_service.set("metro_stations_static", stations, ttl=3600*24*7)
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] _build_and_cache_static_stations() -> {len(stations)} stations ({elapsed:.4f} s)")
        return stations

    async def _build_and_cache_station_alerts(self):
        start = time.perf_counter()
        station_alerts = defaultdict(list)
        lines = await self.get_all_lines()
        alert_lines = [line for line in lines if line.has_alerts]

        semaphore = asyncio.Semaphore(10)

        async def process_line(line):
            async with semaphore:
                stations = await self.get_stations_by_line(line.code)
            return [(st.code, st.alerts) for st in stations]

        results = await asyncio.gather(*(process_line(line) for line in alert_lines))

        for station_list in results:
            for code, alerts in station_list:
                station_alerts[code].extend(alerts)

        alerts_dict = dict(station_alerts)
        await self.cache_service.set("metro_stations_alerts", alerts_dict, ttl=3600)
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] _build_and_cache_station_alerts() -> {len(alerts_dict)} stations with alerts ({elapsed:.4f} s)")
        return alerts_dict

        '''
