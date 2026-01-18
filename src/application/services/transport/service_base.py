from abc import abstractmethod
import asyncio
from collections import defaultdict
from typing import Callable, Any, Dict, List
from rapidfuzz import process, fuzz
from src.domain.models.common.station import Station
from src.infrastructure.mappers.line_mapper import LineMapper
from src.domain.schemas.models import LineModel
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
            
            final_lines.append(line)

        final_lines.sort(key=Utils.sort_lines)
        
        elapsed = time.perf_counter() - start
        print(f"[{self.__class__.__name__}] get_enriched_lines -> {len(final_lines)} lines ({elapsed:.4f}s)")
        
        return final_lines
    
    async def sync_lines(self, transport_type: TransportType):
        raw_lines = await self.fetch_lines()

        db_models = []
        for raw in raw_lines:
            db_id = f"{transport_type.value}-{raw.id}"            

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
                extra_data={"category": raw.category} if raw.category else None
            )
            db_models.append(model)

        await self.line_repository.upsert_many(db_models)
        print(f"✅ {len(db_models)} {transport_type.value} lines sync in DB.")
    
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
    async def fetch_stations_by_line(self, line_id: str) -> List[Station]:
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