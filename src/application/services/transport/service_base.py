from abc import abstractmethod
import asyncio
from collections import defaultdict
from typing import Callable, Any, Dict, List
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
import time

class ServiceBase:
    """
    Base class for services that use optional caching and logging.
    """

    def __init__(self, cache_service: CacheService = None, user_data_manager: UserDataManager = None):
        self.line_repository = LineRepository(async_session_factory)
        self.stations_repository = StationsRepository(async_session_factory)
        self.cache_service = cache_service
        self.user_data_manager = user_data_manager

    async def get_all_lines(self, transport_type: TransportType) -> List[Line]:
        start = time.perf_counter()
        t_type_value = transport_type.value
        alerts_cache_key = f"{t_type_value}_alerts_map"

        lines_task = self.line_repository.get_all(t_type_value)
        alerts_task = self._get_alerts_map(transport_type, alerts_cache_key)

        db_lines, alerts_dict = await asyncio.gather(lines_task, alerts_task)

        if not db_lines:
            return []

        final_lines = []
        for model in db_lines:
            line = Line.model_validate(model)
            line_alerts = alerts_dict.get(line.name, [])
            line.has_alerts = len(line_alerts) > 0
            line.alerts = line_alerts
            line.id = model.original_id

            if model.extra_data and not line.category:
                line.category = model.extra_data.get('category')
            
            final_lines.append(line)

        final_lines.sort(key=Utils.sort_lines)
        
        elapsed = time.perf_counter() - start
        print(f"[{self.__class__.__name__}] get_all_lines -> {len(final_lines)} lines ({elapsed:.4f}s)")
        
        return final_lines
    
    async def get_stations_by_line_code(self, transport_type: TransportType, line_code: str) -> List[Station]:
        start = time.perf_counter()

        # Lanzamos tareas en paralelo (DB y Cache de Alertas)
        stations_task = self.stations_repository.get_by_line_id(f"{transport_type.value}-{line_code}")
        alerts_key = f"{transport_type.value}_alerts_map"
        alerts_task = self._get_alerts_map(transport_type, alerts_key)

        db_stations, alerts_dict = await asyncio.gather(stations_task, alerts_task)

        if not db_stations:
            return []

        final_stations = []
        
        for model in db_stations:
            station = self._map_db_to_domain(model)

            station_alerts = alerts_dict.get(station.name, [])
            station.has_alerts = len(station_alerts) > 0
            station.alerts = station_alerts

            final_stations.append(station)

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stations_by_line({line_code}) -> {len(final_stations)} stations ({elapsed:.4f}s)")
        
        return final_stations
    
    async def get_stations_by_name(self, station_name: str, transport_type: TransportType) -> List[Station]:
        start = time.perf_counter()
        
        cache_key = f"all_stations_{transport_type.value}"

        # Función interna para cachear
        async def fetch_and_map_all_stations():
            models = await self.stations_repository.get_by_transport_type(transport_type.value)
            # ✅ USAMOS EL HELPER (Comprensión de lista limpia)
            return [self._map_db_to_domain(model) for model in models]

        # 1. Obtener todas (Cache o DB)
        all_stations = await self._get_from_cache_or_api(
            cache_key=cache_key,
            api_call=fetch_and_map_all_stations,
            cache_ttl=86400 # 24 horas
        )

        # 2. Filtrar
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

    async def sync_lines(self, transport_type: TransportType):
        raw_lines = await self.fetch_lines()
        print(f"⏳ {len(raw_lines)} {transport_type.value} lines to be sync in DB.")

        LINE_DB_COLUMNS = {
            'id', 'original_id', 'code', 'name', 'description',
            'latitude', 'longitude', 'transport_type', 'stations',
            'destination', 'origin', 'color', 'extra_data'
        }

        db_models = []
        for raw in raw_lines:
            all_attributes = raw.model_dump()
            dynamic_extra = {
                key: value 
                for key, value in all_attributes.items() 
                if key not in LINE_DB_COLUMNS and value is not None
            }
            db_id = f"{transport_type.value}-{raw.code}"

            if transport_type == TransportType.TRAM:
                line_stops = await self.fetch_stations_by_line(raw.id)
                raw.origin = line_stops[0].name
                raw.destination = line_stops[-1].name
                raw.description = f"{line_stops[0].name} - {line_stops[-1].name}"

            model = LineModel(
                id=db_id,
                original_id=str(raw.id),
                code=str(raw.code),
                name=raw.name,
                description=raw.description,
                origin=raw.origin,
                destination=raw.destination,
                transport_type=transport_type.value,
                color=LineMapper.resolve_color(raw.name, transport_type, raw.color),
                extra_data=dynamic_extra or None
            )
            db_models.append(model)

        await self.line_repository.upsert_many(db_models)
        print(f"✅ {len(db_models)} {transport_type.value} lines sync in DB.")

    async def sync_stations(self, transport_type: TransportType):
        raw_stations = await self.fetch_stations()
        total = len(raw_stations)
        print(f"⏳ {total} {transport_type.value} stations found. Starting sync...")

        STATION_DB_COLUMNS = {
            'id', 'original_id', 'code', 'name', 'description',
            'latitude', 'longitude', 'transport_type', 'order', 
            'line_id', 'connections_data', 'extra_data'
        }

        batch_size = 500
        current_batch = []
        
        count = 0

        for raw in raw_stations:
            all_attributes = raw.model_dump()
            dynamic_extra = {
                key: value 
                for key, value in all_attributes.items() 
                if key not in STATION_DB_COLUMNS and value is not None
            }
            
            db_id = f"{transport_type.value}-{raw.line_code}-{raw.id}"

            model = StationModel(
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
                extra_data=dynamic_extra or None
            )
            current_batch.append(model)

            if len(current_batch) >= batch_size:
                try:
                    await self.stations_repository.upsert_many(current_batch)
                    count += len(current_batch)
                    print(f"   ↳ Guardadas {count}/{total} estaciones...")
                    current_batch = []
                except Exception as e:
                    print(f"❌ Error guardando lote: {e}")

        if current_batch:
            try:
                await self.stations_repository.upsert_many(current_batch)
                count += len(current_batch)
                print(f"   ↳ Guardadas {count}/{total} estaciones (Final).")
            except Exception as e:
                print(f"❌ Error guardando último lote: {e}")

        print(f"✅ Sync finalizada: {count} {transport_type.value} stations en DB.")

    async def _get_alerts_map(self, transport_type: TransportType, cache_key: str) -> Dict[str, List[Alert]]:
        cached = await self.cache_service.get(cache_key)
        if cached:
            return cached

        try:
            raw_alerts = await self.fetch_alerts()
            
            result = defaultdict(list)
            for alert in raw_alerts:
                await self.user_data_manager.register_alert(transport_type.value, alert)
                
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
    
    @abstractmethod
    async def fetch_alerts(self) -> List[Alert]:
        pass
    
    @abstractmethod
    async def fetch_lines(self) -> List[Line]:
        pass

    @abstractmethod
    async def fetch_stations(self) -> List[Station]:
        pass

    @abstractmethod
    async def fetch_stations_by_line(self, line_id: str) -> List[Station]:
        pass

    @abstractmethod
    async def fetch_station_connections(self, station: Station) -> Connections:
        pass

    def log_exec_time(func):
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = await func(*args, **kwargs)
            elapsed = (time.perf_counter() - start) * 1000
            cls = args[0].__class__.__name__ if args else "Unknown"
            logger.debug(f"[{cls}] {func.__name__} ejecutado en {elapsed:.2f} ms")
            return result
        return wrapper

    def fuzzy_search(
        self,
        query: str,
        items: List[Any],
        key: Callable[[Any], str],
        threshold: float = 80
    ) -> List[Any]:
        """
        Performs fuzzy search on a list of objects, returning all exact matches
        plus all fuzzy matches above the threshold.

        Args:
            query: Text to search.
            items: List of objects.
            key: Function to extract the text field from each object.
            threshold: Minimum similarity (0-100) for fuzzy match.

        Returns:
            List of objects matching the query exactly or approximately.
        """
        query_lower = query.lower()

        # --- Exact matches (substring, case-insensitive) ---
        exact_matches = [item for item in items if query_lower in key(item).lower()]

        # --- Matches without special chars ---
        remaining_items = [item for item in items if item not in exact_matches]
        normalized_matches = [item for item in remaining_items if HtmlHelper.normalize_text(query_lower) in HtmlHelper.normalize_text(key(item).lower())]

        # --- Prepare fuzzy search excluding exact matches ---
        remaining_items = [item for item in items if item not in (exact_matches + normalized_matches)]
        item_dict = {key(item): item for item in remaining_items}

        # --- Fuzzy matches ---
        fuzzy_matches = process.extract(
            query=query,
            choices=item_dict.keys(),
            scorer=fuzz.WRatio
        )

        fuzzy_filtered = [item_dict[name] for name, score, _ in fuzzy_matches if score >= threshold]

        # --- Combine exact + normalized + fuzzy ---
        return exact_matches + normalized_matches + fuzzy_filtered

    async def _get_from_cache_or_data(
        self,
        cache_key: str,
        data: Any,
        cache_ttl: int = 3600
    ) -> Any:
        """
        Generic method to fetch data from cache or use the provided data,
        then store it in cache if needed.

        Args:
            cache_key: Key to use for caching.
            data: Pre-fetched or pre-computed data.
            cache_ttl: Time to live for the cache in seconds.

        Returns:
            The data, either from cache or the provided one.
        """
        class_name = self.__class__.__name__

        if self.cache_service:
            cached_data = await self.cache_service.get(cache_key)
            if cached_data:
                logger.debug(f"[{class_name}] Cache hit: {cache_key}")
                return cached_data
            else:
                logger.debug(f"[{class_name}] Cache miss: {cache_key}")

        # Store provided data in cache
        if self.cache_service and data is not None:
            await self.cache_service.set(cache_key, data, ttl=cache_ttl)
            logger.debug(f"[{class_name}] Cached data for key: {cache_key} (TTL={cache_ttl}s)")

        return data

    async def _get_from_cache_or_api(
        self,
        cache_key: str,
        api_call: Callable[[], Any],
        cache_ttl: int = 3600
    ) -> Any:
        """
        Fetch data from cache or, if not present, call the API function
        and store the result in cache using the base helper.

        Args:
            cache_key: Key to use for caching.
            api_call: Async callable that fetches the data.
            cache_ttl: Time to live for the cache in seconds.

        Returns:
            Data from cache or API.
        """
        class_name = self.__class__.__name__

        # Try cache first
        if self.cache_service:
            cached_data = await self.cache_service.get(cache_key)
            if cached_data:
                logger.debug(f"[{class_name}] Cache hit: {cache_key}")
                return cached_data
            else:
                logger.debug(f"[{class_name}] Cache miss: {cache_key}")

        # Fetch data from API
        try:
            data = await api_call()
            logger.debug(f"[{class_name}] Fetched data from API for key: {cache_key}")
        except Exception as e:
            logger.error(f"[{class_name}] Error fetching data for key {cache_key}: {e}")
            data = []

        # Use the generic method to cache and return
        return await self._get_from_cache_or_data(cache_key, data, cache_ttl)
    
    def _map_db_to_domain(self, model) -> Station:
        """
        Convierte un StationModel (SQLAlchemy) a Station (Pydantic),
        rellenando datos desde relaciones y extra_data.
        """
        # 1. Validación base
        st = Station.model_validate(model)

        # 2. Hidratar desde la Relación 'Line' (JOIN)
        if model.line:
            st.line_name = model.line.name
            st.line_code = model.line.code

        # 3. Hidratar desde 'extra_data' (Fallback y campos específicos)
        if model.extra_data:
            # Fallback de línea
            if not st.line_name: st.line_name = model.extra_data.get('line_name')
            if not st.line_code: st.line_code = model.extra_data.get('line_code')
            
            # IDs Específicos (Rodalies/FGC/Tram)
            # 🛑 CORREGIDO: Antes asignabas todo a moute_id por error
            if not st.moute_id:      st.moute_id = model.extra_data.get('moute_id')
            if not st.outbound_code: st.outbound_code = model.extra_data.get('outboundCode')
            if not st.return_code:   st.return_code = model.extra_data.get('returnCode')
            if not st.station_group_code: st.station_group_code = model.extra_data.get('station_group_code')
            if not st.direction: st.direction = model.extra_data.get('direction')

        # 4. Hidratar Conexiones
        if model.connections_data and not st.connections:
            try:
                # Importación local para evitar ciclos
                from src.domain.models.common.connections import Connections
                st.connections = Connections.model_validate(model.connections_data)
            except Exception as e:
                logger.warning(f"Error parsing connections for {st.code}: {e}")

        return st