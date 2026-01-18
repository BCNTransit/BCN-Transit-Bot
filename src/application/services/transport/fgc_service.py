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
