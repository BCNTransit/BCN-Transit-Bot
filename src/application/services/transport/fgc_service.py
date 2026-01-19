import asyncio
import time
from typing import List, Optional

# Domain Models
from src.domain.models.common.station import Station
from src.domain.models.common.line import Line
from src.domain.models.common.line_route import LineRoute
from src.domain.models.common.next_trip import NextTrip, normalize_to_seconds
from src.domain.models.common.alert import Alert
from src.domain.models.common.connections import Connections
from src.domain.enums.transport_type import TransportType

# Infrastructure & App
from src.infrastructure.external.api.fgc_api_service import FgcApiService
from src.application.services.user_data_manager import UserDataManager
from src.infrastructure.localization.language_manager import LanguageManager
from src.application.services.cache_service import CacheService
from src.core.logger import logger
from .service_base import ServiceBase

class FgcService(ServiceBase):

    def __init__(
        self,
        fgc_api_service: FgcApiService,
        language_manager: LanguageManager,
        cache_service: CacheService = None,
        user_data_manager: UserDataManager = None
    ):
        super().__init__(cache_service, user_data_manager)
        self.fgc_api_service = fgc_api_service
        self.language_manager = language_manager
        logger.info(f"[{self.__class__.__name__}] FgcService initialized")

    # =========================================================================
    # 🔄 MÉTODOS DE SINCRONIZACIÓN (SEEDER)
    # =========================================================================

    async def sync_lines(self):
        await super().sync_lines(TransportType.FGC)

    async def sync_stations(self):
        await super().sync_stations(TransportType.FGC)

    # --- Implementación de Abstract Methods ---

    async def fetch_lines(self) -> List[Line]:
        return await self.fgc_api_service.get_all_lines()

    async def fetch_stations(self) -> List[Station]:
        api_stations = []
        
        lines = await self.line_repository.get_all(TransportType.FGC.value)
        if not lines:
            lines = await self.fetch_lines()

        semaphore_api = asyncio.Semaphore(5)
        
        async def fetch_line_stations(line_id):
            async with semaphore_api:
                return await self.fetch_stations_by_line(line_id)

        raw_results = await asyncio.gather(*[fetch_line_stations(l.original_id or l.id) for l in lines])
        all_raw_stations = [s for sublist in raw_results for s in sublist]

        print(f"🌍 Calculando IDs de tiempo real (Moute) para {len(all_raw_stations)} estaciones FGC...")        
        semaphore_geo = asyncio.Semaphore(10)

        async def enrich_station(station: Station):
            async with semaphore_geo:
                moute_data = await self.fgc_api_service.get_near_stations(
                    station.latitude, station.longitude
                )
                
                if moute_data:                    
                    station.moute_id = moute_data[0].get("id")
                else:
                    logger.warning(f"⚠️ No Moute ID found for FGC station: {station.name}")
                
                return station

        enriched_stations = await asyncio.gather(*[enrich_station(s) for s in all_raw_stations])
        
        return enriched_stations

    async def fetch_stations_by_line(self, line_id: str) -> List[Station]:
        return await self.fgc_api_service.get_stations_by_line(line_id)

    async def fetch_alerts(self) -> List[Alert]:
        # TODO: Implementar alertas reales de FGC si existen API
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
        return next((l for l in lines if str(l.code) == str(line_id) or l.id == line_id), None)

    # =========================================================================
    # ⚡ MÉTODOS REAL-TIME
    # =========================================================================

    async def get_station_routes(self, station_code: str) -> List[LineRoute]:
        """
        Obtiene próximos trenes.
        Usa 'moute_id' (guardado en DB) para tiempo real.
        Fallback a scraping/horario si falla.
        """
        start = time.perf_counter()
        
        # 1. Recuperar estación de DB
        station = await self.get_station_by_code(station_code)
        if not station: return []

        routes = await self._get_from_cache_or_data(
            f"fgc_station_{station_code}_routes", None, cache_ttl=30
        )

        if routes is not None:
            return routes

        # 2. Lógica de Tiempo Real
        moute_id = station.extra_data.get('moute_id') if station.extra_data else None
        
        # Opción A: Usar API oficial "Mou-te" (Tiempo Real preciso)
        if moute_id:
            raw_routes = await self.fgc_api_service.get_moute_next_departures(moute_id)
            routes = self._map_moute_response(raw_routes)
        
        # Opción B: Fallback (Scraping o estimación)
        else:
            # Asumimos que get_next_departures hace scraping o usa otra fuente
            # station.line_name podría necesitar ajuste si guardaste el nombre completo en DB
            line_name_clean = station.extra_data.get('line_original_name') or station.line.name if station.line else ""
            raw_routes = await self.fgc_api_service.get_next_departures(station.name, line_name_clean)
            routes = self._map_fallback_response(raw_routes, station)

        # Cachear resultado
        await self.cache_service.set(f"fgc_station_{station_code}_routes", routes, ttl=30)
        
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_station_routes({station_code}) -> {len(routes)} routes ({elapsed:.4f}s)")
        return routes

    def _map_moute_response(self, raw_routes: dict) -> List[LineRoute]:
        """Helper para transformar respuesta de Moute a LineRoute."""
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
                    route_id=line_name
                ))
        return routes

    def _map_fallback_response(self, raw_routes: dict, station: Station) -> List[LineRoute]:
        """Helper para transformar respuesta fallback."""
        routes = []
        for direction, trips in raw_routes.items():
            next_trips = [
                NextTrip(
                    id=trip.get("trip_id"), 
                    arrival_time=normalize_to_seconds(trip.get("departure_time"))
                )
                for trip in trips
            ]
            # Intentamos usar info de la línea si la tenemos cargada
            l_name = station.line.name if station.line else "FGC"
            
            routes.append(LineRoute(
                destination=direction,
                next_trips=next_trips,
                line_name=l_name,
                line_type=TransportType.FGC,
                route_id=l_name
            ))
        return routes

    async def get_fgc_station_connections(self, station_code: str) -> Connections:
        """Devuelve conexiones desde el objeto Station."""
        station = await self.get_station_by_code(station_code)
        if station and station.connections:
            return station.connections
        return Connections(lines=[])





'''
import asyncio
import time
from typing import List

from src.domain.models.common.alert import Alert
from src.domain.models.common.connections import Connections
from src.domain.models.common.line import Line
from src.domain.models.fgc.fgc_station import FgcStation
from src.domain.models.common.line_route import LineRoute
from src.domain.models.common.next_trip import NextTrip, normalize_to_seconds
from src.infrastructure.external.api.fgc_api_service import FgcApiService
from src.infrastructure.localization.language_manager import LanguageManager
from src.core.logger import logger


from src.application.services.cache_service import CacheService
from src.application.services.user_data_manager import UserDataManager
from src.domain.enums.transport_type import TransportType
from src.infrastructure.mappers.line_mapper import LineMapper
from .service_base import ServiceBase


class FgcService(ServiceBase):
    """
    Service to interact with Metro data via TmbApiService, with optional caching.
    """

    def __init__(
        self,
        fgc_api_service: FgcApiService,
        language_manager: LanguageManager,
        cache_service: CacheService = None,
        user_data_manager: UserDataManager = None
    ):
        super().__init__(cache_service, user_data_manager)
        self.fgc_api_service = fgc_api_service
        self.language_manager = language_manager
        self.user_data_manager = user_data_manager
        logger.info(f"[{self.__class__.__name__}] FgcService initialized")

    async def get_all_lines(self) -> List[Line]:
        return await super().get_all_lines(TransportType.FGC)
    
    async def fetch_alerts(self) -> List[Alert]:
        return []  # TODO: FGC alerts not implemented yet

    async def fetch_lines(self) -> List[Line]:
        return await self.fgc_api_service.get_all_lines()

    async def sync_lines(self):
        await super().sync_lines(TransportType.FGC)    

    async def get_all_stations(self) -> List[FgcStation]:
        start = time.perf_counter()
        fgc_stations_key = "fgc_stations"
        cached_stations = await self._get_from_cache_or_data(
            fgc_stations_key, None, cache_ttl=3600 * 24
        )

        if cached_stations is not None:
            elapsed = (time.perf_counter() - start)
            logger.info(f"[{self.__class__.__name__}] get_all_stations (cache hit) ejecutado en {elapsed:.4f} s")
            return cached_stations

        lines = await self.get_all_lines()
        stations = []

        semaphore_lines = asyncio.Semaphore(5)
        semaphore_near = asyncio.Semaphore(10)

        async def process_station(line_station: FgcStation, line: Line):
            async with semaphore_near:
                line_station = FgcStation.update_line_info(line_station, line)
                moute_station = await self.fgc_api_service.get_near_stations(
                    line_station.latitude, line_station.longitude
                )
                if moute_station:
                    line_station.moute_id = moute_station[0].get("id")
                return line_station

        async def process_line(line: Line):
            async with semaphore_lines:
                line_stations = await self.fgc_api_service.get_stations_by_line(line.id)
            processed_stations = await asyncio.gather(
                *[process_station(s, line) for s in line_stations]
            )
            return processed_stations

        results = await asyncio.gather(*[process_line(line) for line in lines])
        for line_stations in results:
            stations.extend(line_stations)

        logger.warning(
            f"The following FGC stations where not found:\n "
            f"{[s for s in stations if s.moute_id is None]}"
        )
        result = await self._get_from_cache_or_data(
            fgc_stations_key, stations, cache_ttl=3600 * 24
        )
        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] get_all_stations ejecutado en {elapsed:.4f} s")
        return result

    async def get_stations_by_line(self, line_id) -> List[FgcStation]:
        start = time.perf_counter()
        stations = await self.get_all_stations()
        result = [s for s in stations if s.line_id == line_id]
        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] get_stations_by_line({line_id}) ejecutado en {elapsed:.4f} s")
        return result

    async def get_stations_by_name(self, station_name) -> List[FgcStation]:
        start = time.perf_counter()
        stations = await self.get_all_stations()
        if station_name == "":
            elapsed = (time.perf_counter() - start)
            logger.info(f"[{self.__class__.__name__}] get_stations_by_name(empty) ejecutado en {elapsed:.4f} s")
            return stations
        result = self.fuzzy_search(
            query=station_name,
            items=stations,
            key=lambda station: station.name
        )
        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] get_stations_by_name({station_name}) ejecutado en {elapsed:.4f} s")
        return result

    async def get_station_routes(self, station_code) -> List[LineRoute]:
        start = time.perf_counter()
        station = await self.get_station_by_code(station_code)

        routes = await self._get_from_cache_or_data(
            f"fgc_station_{station_code}_routes",
            None,
            cache_ttl=30
        )

        if routes is None:
            if station.moute_id is not None:
                raw_routes = await self.fgc_api_service.get_moute_next_departures(station.moute_id)
                routes = []
                for line, destinations in raw_routes.items():
                    for destination, trips in destinations.items():
                        nextFgc = [
                            NextTrip(
                                id="",
                                arrival_time=normalize_to_seconds(int(trip.get("departure_time"))),
                            )
                            for trip in trips
                        ]
                        routes.append(
                            LineRoute(
                                destination=destination,
                                next_trips=nextFgc,
                                line_name=line,
                                line_id=line,
                                line_code=line,
                                line_type=TransportType.FGC,
                                color=None,
                                route_id=line,
                            )
                        )
            else:
                raw_routes = await self.fgc_api_service.get_next_departures(station.name, station.line_name)
                routes = []
                for direction, trips in raw_routes.items():
                    nextFgc = [
                        NextTrip(
                            id=trip.get("trip_id"),
                            arrival_time=normalize_to_seconds(trip.get("departure_time")),
                        )
                        for trip in trips
                    ]
                    routes.append(
                        LineRoute(
                            destination=direction,
                            next_trips=nextFgc,
                            line_name=station.line_name,
                            line_id=station.line_name,
                            line_code=station.line_name,
                            line_type=TransportType.FGC,
                            color=None,
                            route_id=station.line_name,
                        )
                    )

            routes = await self._get_from_cache_or_data(
                f"fgc_station_{station_code}_routes",
                routes,
                cache_ttl=30,
            )
        
        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] get_station_routes({station_code}) ejecutado en {elapsed:.4f} s")
        return routes

    async def get_station_by_id(self, station_id, line_id) -> FgcStation:
        start = time.perf_counter()
        stations = await self.get_stations_by_line(line_id)
        station = next((s for s in stations if str(s.id) == str(station_id)), None)
        elapsed = (time.perf_counter() - start)
        logger.info(
            f"[{self.__class__.__name__}] get_station_by_id({station_id}, line {line_id}) "
            f"-> {station} ejecutado en {elapsed:.4f} s"
        )
        return station
    
    async def get_station_by_code(self, station_code) -> FgcStation:
        start = time.perf_counter()
        stations = await self.get_all_stations()
        station = next((s for s in stations if str(s.code) == str(station_code)), None)
        elapsed = (time.perf_counter() - start)
        logger.info(
            f"[{self.__class__.__name__}] get_station_by_id({station_code}) "
            f"-> {station} ejecutado en {elapsed:.4f} s"
        )
        return station

    async def get_line_by_id(self, line_id) -> Line:
        start = time.perf_counter()
        lines = await self.get_all_lines()
        line = next((l for l in lines if str(l.id) == str(line_id)), None)
        elapsed = (time.perf_counter() - start)
        logger.info(
            f"[{self.__class__.__name__}] get_line_by_id({line_id}) "
            f"-> {line} ejecutado en {elapsed:.4f} s"
        )
        return line
    
    async def get_fgc_station_connections(self, station_code) -> Connections:
        start = time.perf_counter()
        connections = await self.cache_service.get(f"fgc_station_connections_{station_code}")
        if connections:
            elapsed = (time.perf_counter() - start)
            logger.info(f"[{self.__class__.__name__}] get_fgc_station_connections({station_code}) from cache -> {len(connections)} connections (tiempo: {elapsed:.4f} s)")
            return connections
        
        same_stops = [s for s in await self.get_all_stations() if s.code == station_code]
        connections = [LineMapper.map_fgc_connection(s.line_id, s.line_code, s.line_name, s.line_description, s.line_color) for s in same_stops]
        await self.cache_service.set(f"fgc_station_connections_{station_code}", connections, ttl=3600*24)

        elapsed = (time.perf_counter() - start)
        logger.info(f"[{self.__class__.__name__}] get_fgc_station_connections({station_code}) from cache -> {len(connections)} connections (tiempo: {elapsed:.4f} s)")
        return connections

        '''
