from abc import abstractmethod
import asyncio
from collections import defaultdict
from typing import Callable, Any, Dict, List, Set, TypeVar
import time

from rapidfuzz import process, fuzz

from src.domain.models.common.connections import Connections
from src.infrastructure.database.repositories.stations_repository import StationsRepository
from src.domain.models.common.station import Station
from src.infrastructure.mappers.line_mapper import LineMapper
from src.domain.schemas.models import LineModel, StationModel
from src.application.services.user_data_manager import UserDataManager
from src.domain.models.common.alert import Alert
from src.application.utils.utils import Utils
from src.domain.enums.transport_type import TransportType
from src.domain.models.common.line import Line
from src.infrastructure.database.repositories.line_repository import LineRepository
from src.infrastructure.database.database import async_session_factory
from src.core.logger import logger
from src.application.utils.html_helper import HtmlHelper
from src.application.services.cache_service import CacheService

T = TypeVar("T")

class ServiceBase:

    def __init__(self, cache_service: CacheService = None, user_data_manager: UserDataManager = None):
        self.line_repository = LineRepository(async_session_factory)
        self.stations_repository = StationsRepository(async_session_factory)
        self.cache_service = cache_service
        self.user_data_manager = user_data_manager

    async def get_all_lines(self, transport_type: TransportType) -> List[Line]:
        start = time.perf_counter()
        
        db_lines, alerts_dict = await asyncio.gather(
            self.line_repository.get_all(transport_type.value),
            self._get_alerts_map(transport_type)
        )

        if not db_lines:
            return []

        final_lines = []
        for model in db_lines:
            line = Line.model_validate(model)
            line.id = model.original_id 
            
            if model.extra_data and not line.category:
                line.category = model.extra_data.get('category')
            
            final_lines.append(line)

        self._enrich_with_alerts(final_lines, alerts_dict, key_attr="name")

        final_lines.sort(key=Utils.sort_lines)
        
        elapsed = time.perf_counter() - start
        print(f"[{self.__class__.__name__}] get_all_lines -> {len(final_lines)} lines ({elapsed:.4f}s)")
        return final_lines

    async def get_stations_by_line_code(self, transport_type: TransportType, line_code: str) -> List[Station]:
        start = time.perf_counter()

        db_stations, alerts_dict = await asyncio.gather(
            self.stations_repository.get_by_line_id(f"{transport_type.value}-{line_code}"),
            self._get_alerts_map(transport_type)
        )

        if not db_stations:
            return []

        final_stations = [self._map_db_to_domain(model) for model in db_stations]
        self._enrich_with_alerts(final_stations, alerts_dict, key_attr="name")

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stations_by_line({line_code}) -> {len(final_stations)} stations ({elapsed:.4f}s)")
        return final_stations

    async def get_stations_by_name(self, station_name: str, transport_type: TransportType) -> List[Station]:
        start = time.perf_counter()
        cache_key = f"all_stations_{transport_type.value}"

        async def fetch_and_map():
            models = await self.stations_repository.get_by_transport_type(transport_type.value)
            return [self._map_db_to_domain(model) for model in models]

        stations_task = self._get_from_cache_or_api(
            cache_key=cache_key,
            api_call=fetch_and_map,
            cache_ttl=86400
        )
        alerts_task = self._get_alerts_map(transport_type)

        all_stations, alerts_dict = await asyncio.gather(stations_task, alerts_task)

        if not station_name:
            result = all_stations
        else:
            result = self.fuzzy_search(
                query=station_name, items=all_stations, key=lambda s: s.name, threshold=75
            )

        self._enrich_with_alerts(result, alerts_dict, key_attr="name")

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stations_by_name('{station_name}') -> {len(result)} matches ({elapsed:.4f}s)")
        return result

    async def sync_lines(self, transport_type: TransportType):
        raw_lines = await self.fetch_lines()
        print(f"⏳ {len(raw_lines)} {transport_type.value} lines to be sync in DB.")

        VALID_COLS = {
            'id', 'original_id', 'code', 'name', 'description',
            'latitude', 'longitude', 'transport_type', 'stations',
            'destination', 'origin', 'color', 'extra_data'
        }

        async def transform_line(raw: Line) -> LineModel:
            db_id = f"{transport_type.value}-{raw.code}"
            
            if transport_type == TransportType.TRAM:
                line_stops = await self.fetch_stations_by_line(raw.id)
                if line_stops:
                    raw.origin = line_stops[0].name
                    raw.destination = line_stops[-1].name
                    raw.description = f"{line_stops[0].name} - {line_stops[-1].name}"

            extra = self._extract_extra_data(raw, VALID_COLS)
            
            return LineModel(
                id=db_id,
                original_id=str(raw.id),
                code=str(raw.code),
                name=raw.name,
                description=raw.description,
                origin=raw.origin,
                destination=raw.destination,
                transport_type=transport_type.value,
                color=LineMapper.resolve_color(raw.name, transport_type, raw.color),
                extra_data=extra
            )

        await self._sync_batch(raw_lines, transform_line, self.line_repository, f"{transport_type.value} lines")

    async def sync_stations(self, transport_type: TransportType):
        raw_stations = await self.fetch_stations()
        print(f"⏳ {len(raw_stations)} {transport_type.value} stations found. Starting sync...")

        VALID_COLS = {
            'id', 'original_id', 'code', 'name', 'description',
            'latitude', 'longitude', 'transport_type', 'order', 
            'line_id', 'connections_data', 'extra_data'
        }

        async def transform_station(raw: Station) -> StationModel:
            db_id = f"{transport_type.value}-{raw.line_code}-{raw.id}"
            extra = self._extract_extra_data(raw, VALID_COLS)
            
            return StationModel(
                id=db_id,
                original_id=str(raw.id),
                code=str(raw.code),
                name=raw.name,
                description=raw.description,
                latitude=raw.latitude,
                longitude=raw.longitude,
                transport_type=transport_type.value,
                order=raw.order,
                line_id=f"{transport_type.value}-{raw.line_code}",
                connections_data=None, 
                extra_data=extra
            )

        await self._sync_batch(raw_stations, transform_station, self.stations_repository, f"{transport_type.value} stations")

    async def _sync_batch(self, raw_items: List[Any], transform_func: Callable[[Any], Any], repository: Any, label: str):
        batch_size = 500
        current_batch = []
        count = 0
        total = len(raw_items)

        for raw in raw_items:
            if asyncio.iscoroutinefunction(transform_func):
                model = await transform_func(raw)
            else:
                model = transform_func(raw)
                
            current_batch.append(model)

            if len(current_batch) >= batch_size:
                await self._safe_upsert(repository, current_batch, label)
                count += len(current_batch)
                print(f"   ↳ Guardadas {count}/{total} {label}...")
                current_batch = []

        if current_batch:
            await self._safe_upsert(repository, current_batch, label)
            count += len(current_batch)

        print(f"✅ Sync finalizada: {count} {label} en DB.")

    async def _safe_upsert(self, repository, batch, label):
        try:
            await repository.upsert_many(batch)
        except Exception as e:
            print(f"❌ Error guardando lote de {label}: {e}")

    def _extract_extra_data(self, obj: Any, valid_columns: Set[str]) -> Dict:
        return {
            key: value 
            for key, value in obj.model_dump().items() 
            if key not in valid_columns and value is not None
        }

    def _enrich_with_alerts(self, items: List[Any], alerts_map: Dict[str, List[Alert]], key_attr: str = "name"):
        for item in items:
            key = getattr(item, key_attr, "")
            relevant_alerts = alerts_map.get(key, [])
            item.alerts = relevant_alerts
            item.has_alerts = len(relevant_alerts) > 0

    async def _get_alerts_map(self, transport_type: TransportType) -> Dict[str, List[Alert]]:
        cache_key = f"{transport_type.value}_alerts_map"
        
        cached = await self.cache_service.get(cache_key)
        if cached:
            return cached

        try:
            raw_alerts = await self.fetch_alerts()
            result = defaultdict(list)
            
            for alert in raw_alerts:
                await self.user_data_manager.register_alert(transport_type, alert)
                seen_lines = set()
                for entity in alert.affected_entities:
                    if entity.line_name and entity.line_name not in seen_lines:
                        result[entity.line_name].append(alert)
                        seen_lines.add(entity.line_name)
            
            alerts_dict = dict(result)
            await self.cache_service.set(cache_key, alerts_dict, ttl=3600)
            return alerts_dict

        except Exception as e:
            print(f"❌ Error en alertas ({transport_type.value}): {e}")
            return {}

    async def _get_from_cache_or_api(self, cache_key: str, api_call: Callable[[], Any], cache_ttl: int = 3600) -> Any:
        class_name = self.__class__.__name__

        if self.cache_service:
            cached_data = await self.cache_service.get(cache_key)
            if cached_data:
                logger.debug(f"[{class_name}] Cache hit: {cache_key}")
                return cached_data
            logger.debug(f"[{class_name}] Cache miss: {cache_key}")

        try:
            data = await api_call()
            logger.debug(f"[{class_name}] Fetched data from API for key: {cache_key}")
        except Exception as e:
            logger.error(f"[{class_name}] Error fetching data for key {cache_key}: {e}")
            return []

        if self.cache_service and data:
            await self.cache_service.set(cache_key, data, ttl=cache_ttl)
        
        return data
    
    async def _get_from_cache_or_data(self, cache_key: str, data: Any, cache_ttl: int = 3600) -> Any:
        if self.cache_service:
            cached_data = await self.cache_service.get(cache_key)
            if cached_data: return cached_data
            
        if self.cache_service and data is not None:
             await self.cache_service.set(cache_key, data, ttl=cache_ttl)
             
        return data

    def _map_db_to_domain(self, model) -> Station:
        st = Station.model_validate(model)

        if model.line:
            st.line_name = model.line.name
            st.line_code = model.line.code

        if model.extra_data:
            if not st.line_name: st.line_name = model.extra_data.get('line_name')
            if not st.line_code: st.line_code = model.extra_data.get('line_code')
            if not st.moute_id: st.moute_id = model.extra_data.get('moute_id')
            if not st.outbound_code: st.outbound_code = model.extra_data.get('outboundCode')
            if not st.return_code: st.return_code = model.extra_data.get('returnCode')
            if not st.station_group_code: st.station_group_code = model.extra_data.get('station_group_code')
            if not st.direction: st.direction = model.extra_data.get('direction')

        if model.connections_data and not st.connections:
            try:
                from src.domain.models.common.connections import Connections
                st.connections = Connections.model_validate(model.connections_data)
            except Exception as e:
                logger.warning(f"Error parsing connections for {st.code}: {e}")
            
        return st

    @abstractmethod
    async def fetch_alerts(self) -> List[Alert]: pass
    
    @abstractmethod
    async def fetch_lines(self) -> List[Line]: pass

    @abstractmethod
    async def fetch_stations(self) -> List[Station]: pass

    @abstractmethod
    async def fetch_stations_by_line(self, line_id: str) -> List[Station]: pass

    def log_exec_time(func):
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = await func(*args, **kwargs)
            elapsed = (time.perf_counter() - start) * 1000
            cls = args[0].__class__.__name__ if args else "Unknown"
            logger.debug(f"[{cls}] {func.__name__} ejecutado en {elapsed:.2f} ms")
            return result
        return wrapper

    def fuzzy_search(self, query: str, items: List[Any], key: Callable[[Any], str], threshold: float = 80) -> List[Any]:
        query_lower = query.lower()
        exact_matches = [item for item in items if query_lower in key(item).lower()]
        remaining_items = [item for item in items if item not in exact_matches]
        normalized_matches = [item for item in remaining_items if HtmlHelper.normalize_text(query_lower) in HtmlHelper.normalize_text(key(item).lower())]
        remaining_items = [item for item in items if item not in (exact_matches + normalized_matches)]
        item_dict = {key(item): item for item in remaining_items}
        
        fuzzy_matches = process.extract(
            query=query,
            choices=item_dict.keys(),
            scorer=fuzz.WRatio
        )

        fuzzy_filtered = [item_dict[name] for name, score, _ in fuzzy_matches if score >= threshold]
        return exact_matches + normalized_matches + fuzzy_filtered