from abc import abstractmethod
import asyncio
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from typing import Callable, Any, Dict, List, Optional, Set, TypeVar
import time

from rapidfuzz import process, fuzz

from src.infrastructure.database.repositories.alerts_repository import AlertsRepository
from src.domain.models.common.nearby_station import NearbyStation
from src.domain.models.common.connections import Connections
from src.infrastructure.database.repositories.stations_repository import StationsRepository
from src.domain.models.common.station import Station
from src.infrastructure.mappers.line_mapper import LineMapper
from src.domain.schemas.models import DBAlert, DBLine, DBPhysicalStation, DBRouteStop
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
from sqlalchemy.dialects.postgresql import insert as pg_insert

T = TypeVar("T")

class ServiceBase:

    def __init__(self, cache_service: CacheService = None, user_data_manager: UserDataManager = None):
        self.line_repository = LineRepository(async_session_factory)
        self.stations_repository = StationsRepository(async_session_factory)
        self.alerts_repository = AlertsRepository(async_session_factory)
        self.cache_service = cache_service
        self.user_data_manager = user_data_manager
        self._lines_metadata_cache: Dict[str, DBLine] = {}
        self._cache_last_updated = 0

    async def _ensure_lines_cache(self):
        if self._lines_metadata_cache:
            return

        logger.info("🔄 Pre-loading lines cache for rich connections...")
        
        all_lines = await self.line_repository.get_all(transport_type=None)        
        self._lines_metadata_cache = {
            f"{line.transport_type}-{line.code}": line 
            for line in all_lines
        }
        
        logger.info(f"✅ Lines cache loaded with {len(self._lines_metadata_cache)} unique lines.")

    async def get_all_lines(self, transport_type: TransportType) -> List[Line]:
        start = time.perf_counter()
        
        db_lines, affected_names_set = await asyncio.gather(
            self.line_repository.get_all(transport_type.value),
            self.alerts_repository.get_affected_line_names(transport_type.value) 
        )

        if not db_lines:
            return []

        final_lines = []
        for model in db_lines:
            line = Line.model_validate(model)
            line.id = model.id

            if not line.origin or not line.destination or line.origin == line.destination:
                continue
            
            if model.extra_data and not getattr(line, 'category', None):
                line.category = model.extra_data.get('category')
            
            line.has_alerts = line.name in affected_names_set
            line.alerts = []
            final_lines.append(line)

        final_lines.sort(key=Utils.sort_lines)
        
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_all_lines -> {len(final_lines)} lines ({elapsed:.4f}s)")
        return final_lines

    async def get_line_by_id(self, transport_type: TransportType, line_id: str) -> Line:
        start = time.perf_counter()

        if "-" in line_id:
            try:
                prefix = line_id.split("-")[0].lower()
                id_transport_type = TransportType(prefix)
                
                if id_transport_type != transport_type:
                    logger.error(
                        f"⛔ Type Mismatch: Requested {transport_type} but Line ID belongs to {id_transport_type} ({line_id}). "
                        "Returning empty list to enforce consistency."
                    )
                    return None
                    
            except ValueError:
                return None
        
        db_line = await self.line_repository.get_by_id(line_id)

        if not db_line:
            return None
        
        line = Line.model_validate(db_line)
        line.id = db_line.id

        if not line.origin or not line.destination or line.origin == line.destination:
            return None
        
        if db_line.extra_data and not getattr(line, 'category', None):
            line.category = db_line.extra_data.get('category')

        alerts_map = await self._get_alerts_map(transport_type)
        self._enrich_with_alerts([line], alerts_map, key_attr="name")

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_line_by_id -> {line_id} - {transport_type} ({elapsed:.4f}s)")
        return line

    async def get_stations_by_line_id(self, transport_type: TransportType, line_id: str) -> List[Station]:
        if "-" in line_id:
            try:
                prefix = line_id.split("-")[0].lower()
                id_transport_type = TransportType(prefix)
                
                if id_transport_type != transport_type:
                    logger.error(
                        f"⛔ Type Mismatch: Requested {transport_type} but Line ID belongs to {id_transport_type} ({line_id}). "
                        "Returning empty list to enforce consistency."
                    )
                    return []
                    
            except ValueError:
                return []
            
        await self._ensure_lines_cache()
        line_metadata = self._lines_metadata_cache.get(line_id)
        actual_line_name = line_metadata.name if line_metadata else line_id

        db_results, alerts_dict = await asyncio.gather(
            self.stations_repository.get_by_line_id(line_id),
            self._get_alerts_map(transport_type)
        )

        if not db_results:
            return []

        final_stations = []
        for route_stop in db_results:
            physical = route_stop.station
            extra = physical.extra_data or {}
            
            # 3. Construcción de Conexiones Ricas usando la Caché
            # physical.lines_summary es ["L1", "175"]
            rich_connections = self._build_rich_connections(
                line_entries=physical.lines_summary,
                current_line_name=actual_line_name,
                station_transport_type=transport_type
            )

            domain_obj = Station(
                id=physical.id,
                original_id=physical.id.split('-')[-1] if '-' in physical.id else physical.id,
                code=route_stop.station_external_code or "",
                station_group_code=route_stop.station_group_code,
                name=physical.name,
                latitude=physical.latitude,
                longitude=physical.longitude,
                order=route_stop.order,
                transport_type=transport_type,
                description=physical.description,
                
                # Contexto
                line_code='',
                line_name=getattr(route_stop.line, 'name', ''),
                
                # Extra Data
                moute_id=str(extra.get('moute_id')) if extra.get('moute_id') else None,
                outbound_code=extra.get('outbound_code'),
                return_code=extra.get('return_code'),
                direction=route_stop.direction,
                
                has_alerts=False,
                alerts=[],

                # AQUI ESTÁ LA MAGIA: Pasamos el objeto rico
                connections=rich_connections
            )
            final_stations.append(domain_obj)

        self._enrich_with_alerts(final_stations, alerts_dict, key_attr="name")
        return final_stations
    
    def _build_rich_connections(self, line_entries: List[any], current_line_name: str, station_transport_type: TransportType) -> Optional[dict]:
        if not line_entries:
            return None

        rich_lines = []
        
        type_str = station_transport_type.value if hasattr(station_transport_type, 'value') else str(station_transport_type)
        valid_types = {type_str}
        if type_str == 'nitbus':
            valid_types.add('bus')

        for entry in line_entries:
            # --- 1. NORMALIZACIÓN (El Fix) ---
            # Extraemos el nombre limpio y el color (si viene)
            if isinstance(entry, dict):
                name = entry.get("name", "Unknown")
                fallback_color = entry.get("color", "808080")
            else:
                name = str(entry)
                fallback_color = "808080"

            # 2. Filtro de auto-referencia
            if str(name) == str(current_line_name):
                continue

            # 3. Búsqueda en Caché
            line_data = None
            for cached_line in self._lines_metadata_cache.values():
                if cached_line.name == name and cached_line.transport_type in valid_types:
                    line_data = cached_line
                    break
            
            if line_data:
                rich_lines.append({
                    "id": line_data.id,
                    "code": line_data.code,
                    "name": line_data.name,
                    "description": line_data.description,
                    "origin": line_data.origin,
                    "destination": line_data.destination,
                    "color": line_data.color,
                    "text_color": getattr(line_data, 'text_color', 'FFFFFF'),
                    "transport_type": line_data.transport_type
                })
            else:
                # Fallback visual usando los datos que extrajimos
                rich_lines.append({
                    "id": f"unknown-{name}",
                    "code": name, # Ahora 'name' es string, Pydantic estará feliz
                    "name": name,
                    # Si no está en caché, usamos el color que venía en el summary (¡Mejora visual!)
                    "color": fallback_color,
                    "transport_type": station_transport_type
                })

        if not rich_lines:
            return None

        return {"lines": rich_lines}
    
    async def get_station_by_code(self, station_code: str, transport_type: TransportType) -> Optional[Station]:
        start = time.perf_counter()
        
        cache_key = f"all_stations_{transport_type.value}"
        cached_list = await self.cache_service.get(cache_key)
        
        station = None

        if cached_list:
            station = next((s for s in cached_list if str(s.code) == str(station_code)), None)
            source = "CACHE_LIST"
        else:
            source = "DB_SINGLE_FETCH"
            
            await self._ensure_lines_cache()
            
            db_station = await self.stations_repository.get_by_code(station_code, transport_type.value)
            
            if db_station:
                station = self._map_physical_to_domain(db_station, transport_type)
                alerts_map = await self._get_alerts_map(transport_type)
                self._enrich_with_alerts([station], alerts_map, key_attr="name")

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_station_by_code({station_code}) via {source} -> Found: {station is not None} ({elapsed:.4f}s)")
        
        return station

    async def get_stations_by_name(self, station_name: str, transport_type: TransportType) -> List[NearbyStation]:
        start = time.perf_counter()
        cache_key = f"searchable_route_stops_{transport_type.value}"

        async def fetch_and_map():
            route_stop_models = await self.stations_repository.get_route_stops_with_lines(transport_type.value)
            
            return [
                self._map_route_stop_to_nearby_station(model) 
                for model in route_stop_models
            ]

        all_stops = await self._get_from_cache_or_api(
            cache_key=cache_key,
            api_call=fetch_and_map,
            cache_ttl=86400 
        )

        if not station_name:
            result = all_stops
        else:
            result = self.fuzzy_search(
                query=station_name, 
                items=all_stops, 
                key=lambda s: s.station_name, 
                threshold=75
            )

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stations_by_name('{station_name}') -> {len(result)} route_stops ({elapsed:.4f}s)")
        
        if station_name:
            result.sort(
                key=lambda x: (
                    x.station_name, 
                    x.lines[0].get('name', '') if (x.lines and len(x.lines) > 0) else ""
                )
            )
            
        return result

    async def get_nearby_stations(self, lat: float, lon: float, radius: float, transport_type: TransportType = None, limit: int = 50) -> List[NearbyStation]:
        start = time.perf_counter()
        
        db_results = await self.stations_repository.get_nearby(
            lat=lat, 
            lon=lon, 
            radius_km=radius, 
            transport_type=transport_type,
            limit=limit
        )

        if not db_results:
            return []

        await self._ensure_lines_cache()

        final_results = []
        for db_obj, distance in db_results:            
            nearby = NearbyStation(
                type=db_obj.transport_type,
                station_name=db_obj.name,                
                physical_station_id=db_obj.id,
                coordinates=(db_obj.latitude, db_obj.longitude),
                distance_km=distance,
                lines=db_obj.lines_summary or [],                
                slots=None,
                mechanical=None,
                electrical=None,
                availability=None
            )
            
            final_results.append(nearby)

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] Nearby sync found {len(final_results)} in {elapsed:.4f}s")
        return final_results
    
    async def sync_lines(self, transport_type: TransportType):
        raw_lines = await self.fetch_lines()
        logger.info(f"⏳ {len(raw_lines)} {transport_type.value} lines to be sync in DB.")

        VALID_COLS = {
            'id', 'original_id', 'code', 'name', 'description',
            'latitude', 'longitude', 'transport_type', 'stations',
            'destination', 'origin', 'color', 'extra_data', 'has_alerts', 'alerts', 'name_with_emoji'
        }

        async def transform_line(raw: Line) -> DBLine:
            db_id = f"{transport_type.value}-{raw.code}"
            
            if transport_type == TransportType.TRAM:
                line_stops = await self.fetch_stations_by_line(raw.id)
                if line_stops:
                    raw.origin = line_stops[0].name
                    raw.destination = line_stops[-1].name
                    raw.description = f"{line_stops[0].name} - {line_stops[-1].name}"

            extra = self._extract_extra_data(raw, VALID_COLS)
            
            return DBLine(
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

    async def sync_stations(self, transport_type: TransportType, lines_map: dict = None):
        raw_stations = await self.fetch_stations()
        logger.info(f"⏳ {len(raw_stations)} {transport_type.value} stations found. Starting hybrid sync...")

        # 0. Filtrado usando el lines_map (Whitelist)
        if lines_map:
            original_count = len(raw_stations)
            raw_stations = [
                s for s in raw_stations 
                if f"{transport_type.value}-{s.line_code}" in lines_map
            ]
            diff = original_count - len(raw_stations)
            if diff > 0:
                logger.warning(f"🧹 {transport_type.value}: Se descartaron {diff} estaciones huérfanas (líneas no activas).")

        # --- FASE 1: Procesamiento en Memoria ---
        
        physical_stations_map = {}
        stops_by_line = defaultdict(list)
        dedup_lookup = {} 

        excluded_fields = {
            'id', 'original_id', 'code', 'name', 
            'lat', 'latitude', 'lon', 'longitude', 
            'description', 'municipality', 
            'transport_type', 'type', 
            'line_code', 'line_name', 'order', 'direction', 
            'is_night'
        }

        for raw in raw_stations:
            # 1. Extracción y Limpieza
            extra = self._extract_extra_data(raw, excluded_fields)
            
            t_type_str = transport_type.value if hasattr(transport_type, 'value') else str(transport_type)
            
            if extra:
                for k, v in extra.items():
                    if isinstance(v, TransportType):
                        extra[k] = v.value

            # 2. Gestión del Group Code
            group_code = None
            if extra and 'station_group_code' in extra:
                group_code = extra['station_group_code']
                extra.pop('station_group_code', None)

            # 3. Lógica de Deduplicación
            clean_id = None
            
            if group_code:
                group_key = f"group-{group_code}"
                if group_key in dedup_lookup:
                    clean_id = dedup_lookup[group_key]

            if not clean_id:
                clean_id = dedup_lookup.get((round(raw.latitude, 5), round(raw.longitude, 5)))

            if not clean_id:
                try:
                    number_part = str(int(raw.id))
                except (ValueError, TypeError):
                    number_part = str(raw.id)

                prefix = t_type_str
                if prefix == 'nitbus': prefix = 'bus'
                clean_id = f"{prefix}-{number_part}"
                
                if group_code:
                    dedup_lookup[f"group-{group_code}"] = clean_id
                dedup_lookup[(round(raw.latitude, 5), round(raw.longitude, 5))] = clean_id

            # 4. Construcción del Objeto Estación Física
            if clean_id not in physical_stations_map:
                physical_stations_map[clean_id] = {
                    "id": clean_id,
                    "name": raw.name,
                    "description": raw.description, 
                    "municipality": getattr(raw, 'municipality', None),
                    "lat": raw.latitude,
                    "lon": raw.longitude,
                    "transport_type": t_type_str,
                    "extra_data": extra,
                    "lines_set": set()
                }
            
            # --- 5. Resolución de NOMBRE y COLOR ---
            line_db_id = f"{transport_type.value}-{raw.line_code}"
            
            # Valores por defecto
            final_name = str(raw.line_code)
            final_id = "unknown"
            final_color = "333333" # Gris oscuro por defecto

            if lines_map and line_db_id in lines_map:
                # Caso Ideal: Usamos datos de DB (Nombre limpio + Color oficial)
                entry = lines_map[line_db_id]
                
                # Soportamos tanto si el mapa trae dicts (lo nuevo) como strings (legacy)
                if isinstance(entry, dict):
                    final_name = entry.get("name", final_name)
                    final_id = entry.get("id", final_id)
                    final_color = entry.get("color", final_color)
                else:
                    final_name = str(entry)
            else:
                # Fallback: Heurística de nombre
                raw_name = raw.line_name
                final_name = raw_name if raw_name and len(raw_name) <= 8 else str(raw.line_code)
                
                # Intentamos rescatar color si viene en el objeto raw
                if hasattr(raw, 'color') and raw.color:
                    final_color = raw.color
            
            # IMPORTANTE: Guardamos una TUPLA (nombre, color) en el set para que sea única
            physical_stations_map[clean_id]["lines_set"].add((final_name, final_id, final_color))

            # 6. Preparación de Route Stops
            direction = getattr(raw, 'direction', 'única')
            stops_by_line[(line_db_id, direction)].append((raw, clean_id, group_code))

        # --- FASE 2: Lógica de Ruta ---
        
        route_stops_buffer = []

        for (line_id, direction), stops_tuples in stops_by_line.items():
            sorted_tuples = sorted(stops_tuples, key=lambda x: x[0].order)
            total_stops = len(sorted_tuples)
            
            for index, (stop, s_clean_id, s_group_code) in enumerate(sorted_tuples):
                route_stops_buffer.append(DBRouteStop(
                    line_id=line_id,
                    physical_station_id=s_clean_id,
                    station_external_code=str(stop.code) if stop.code else "",
                    station_group_code=str(s_group_code) if s_group_code else None,
                    order=stop.order,
                    direction=direction,
                    is_origin=(index == 0),
                    is_destination=(index == total_stops - 1)
                ))

        # --- FASE 3: Persistencia Robusta ---
        
        async with async_session_factory() as session:
            try:
                # 3.1 Guardar Estaciones Físicas
                if physical_stations_map:
                    stations_data = []
                    for p_data in physical_stations_map.values():
                        
                        # TRANSFORMACIÓN FINAL: Set de Tuplas -> Lista de Diccionarios
                        # Ordenamos por nombre (x[0]) para que el JSON sea consistente
                        lines_summary_json = [
                            {"name": name, "id": id, "color": color}
                            for name, id, color in sorted(list(p_data["lines_set"]), key=lambda x: x[0])
                        ]

                        stations_data.append({
                            "id": p_data["id"],
                            "name": p_data["name"],
                            "description": p_data["description"],
                            "latitude": p_data["lat"],
                            "longitude": p_data["lon"],
                            "municipality": p_data["municipality"],
                            "transport_type": p_data["transport_type"],
                            "extra_data": p_data["extra_data"],
                            
                            # Aquí guardamos la estructura rica con color
                            "lines_summary": lines_summary_json,
                            
                            "updated_at": datetime.utcnow()
                        })

                    logger.info(f"📍 Upserting {len(stations_data)} physical stations...")
                    BATCH_SIZE = 1000 
                    
                    for i in range(0, len(stations_data), BATCH_SIZE):
                        chunk = stations_data[i : i + BATCH_SIZE]
                        
                        stmt = pg_insert(DBPhysicalStation).values(chunk)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['id'], 
                            set_={
                                "name": stmt.excluded.name,
                                "lines_summary": stmt.excluded.lines_summary,
                                "extra_data": stmt.excluded.extra_data,
                                "updated_at": stmt.excluded.updated_at
                            }
                        )
                        await session.execute(stmt)
                    
                    await session.flush()

                # 3.2 Guardar Route Stops
                if route_stops_buffer:
                    logger.info(f"🚏 Inserting {len(route_stops_buffer)} route stops...")
                    BATCH_SIZE_STOPS = 2000
                    for i in range(0, len(route_stops_buffer), BATCH_SIZE_STOPS):
                        chunk_stops = route_stops_buffer[i : i + BATCH_SIZE_STOPS]
                        session.add_all(chunk_stops)
                        await session.flush() 
                
                await session.commit()
                logger.info(f"✅ {transport_type.value} Sync completed successfully.")

            except Exception as e:
                logger.error(f"❌ Error syncing stations: {e}")
                await session.rollback()
                raise e 

    async def sync_alerts(self, transport_type: TransportType):
        await self.alerts_repository.mark_all_as_inactive(transport_type.value)
        raw_alerts = await self.fetch_alerts()
        logger.info(f"⏳ {len(raw_alerts)} {transport_type.value} alerts to be sync in DB.")

        async def transform_alert(raw: Alert) -> DBAlert:
            return DBAlert(
                external_id=raw.id,                
                transport_type=raw.transport_type.value if hasattr(raw.transport_type, 'value') else raw.transport_type,                
                begin_date=raw.begin_date,
                end_date=raw.end_date,
                status=raw.status,
                cause=raw.cause,                
                publications=[asdict(pub) for pub in raw.publications],                
                affected_entities=[asdict(entity) for entity in raw.affected_entities]
            )
    
        await self._sync_batch(raw_alerts, transform_alert, self.alerts_repository, f"{transport_type.value} alerts")

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
                logger.info(f"   ↳ Guardadas {count}/{total} {label}...")
                current_batch = []

        if current_batch:
            await self._safe_upsert(repository, current_batch, label)
            count += len(current_batch)

        logger.info(f"✅ Sync finalizada: {count} {label} en DB.")

    async def _safe_upsert(self, repository, batch, label):
        try:
            await repository.upsert_many(batch)
        except Exception as e:
            logger.error(f"❌ Error guardando lote de {label}: {e}")

    def _extract_extra_data(self, obj: Any, valid_columns: Set[str]) -> Dict:
        return {
            key: value 
            for key, value in obj.model_dump().items() 
            if key not in valid_columns and value is not None
        }

    def _enrich_with_alerts(self, items: List[Any], alerts_map: Dict[str, List[Alert]], key_attr: str = "name"):
    
        normalized_map = {}
        for key, alerts in alerts_map.items():
            norm_key = key.strip().upper()
            if norm_key not in normalized_map:
                normalized_map[norm_key] = []
            normalized_map[norm_key].extend(alerts)

        for item in items:
            item_alerts = []
            seen_ids = set()
            
            raw_key = getattr(item, key_attr, "")
            search_key = raw_key.strip().upper()
            
            if search_key in normalized_map:
                for alert in normalized_map[search_key]:
                    if alert.id not in seen_ids:
                        item_alerts.append(alert)
                        seen_ids.add(alert.id)
            
            is_station = hasattr(item, 'line_name') and item.line_name
            
            if is_station:
                raw_line_key = item.line_name
                search_line_key = raw_line_key.strip().upper()
                
                if search_line_key != search_key and search_line_key in normalized_map:
                    
                    for alert in normalized_map[search_line_key]:
                        if alert.id in seen_ids:
                            continue

                        targets_specific_stations = any(e.get("station_name") for e in alert.affected_entities)
                        
                        item_name_norm = item.name.strip().upper()
                        
                        targets_me = any(
                            (e.get("station_name") or "").strip().upper() == item_name_norm 
                            for e in alert.affected_entities
                        )

                        if targets_specific_stations and not targets_me:
                            continue

                        item_alerts.append(alert)
                        seen_ids.add(alert.id)

            item.alerts = item_alerts
            item.has_alerts = len(item_alerts) > 0

    async def _get_alerts_map(self, transport_type: TransportType) -> Dict[str, List[Alert]]:
        cache_key = f"{transport_type.value}_alerts_map_db"
        cached = await self.cache_service.get(cache_key)
        if cached: 
            return cached

        try:
            db_alerts = await self.alerts_repository.get_active_alerts(transport_type.value)

            if not db_alerts:
                return {}

            result = defaultdict(list)
            
            for alert_model in db_alerts:
                alert = Alert(
                    id=alert_model.id,
                    transport_type=alert_model.transport_type,
                    begin_date=alert_model.begin_date,
                    end_date=alert_model.end_date,
                    status=alert_model.status,
                    cause=alert_model.cause,
                    publications=alert_model.publications,
                    affected_entities=alert_model.affected_entities
                )
                
                entities = alert.affected_entities or []                
                mapped_keys = set()

                for entity in entities:
                    station_name = entity.get("station_name")
                    if station_name:
                        k = station_name.strip().upper()
                        if k not in mapped_keys:
                            result[k].append(alert)
                            mapped_keys.add(k)
                            
                    line_name = entity.get("line_name")
                    if line_name:
                        k = line_name.strip().upper()
                        if k not in mapped_keys:
                            result[k].append(alert)
                            mapped_keys.add(k)

            alerts_dict = dict(result)

            await self.cache_service.set(cache_key, alerts_dict, ttl=300)
            return alerts_dict

        except Exception as e:
            logger.error(f"Error building alerts map from DB: {e}", exc_info=True) # exc_info ayuda a ver el traceback completo
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
            if not st.outbound_code: st.outbound_code = model.extra_data.get('outbound_code')
            if not st.return_code: st.return_code = model.extra_data.get('return_code')
            if not st.station_group_code: st.station_group_code = model.extra_data.get('station_group_code')
            if not st.direction: st.direction = model.extra_data.get('direction')

        if model.connections_data and not st.connections:
            try:
                from src.domain.models.common.connections import Connections
                st.connections = Connections.model_validate(model.connections_data)
            except Exception as e:
                logger.warning(f"Error parsing connections for {st.code}: {e}")
            
        return st
    
    def _map_physical_to_domain(self, physical: DBPhysicalStation, t_type: TransportType) -> Station:
        extra = physical.extra_data or {}
        
        # Construimos conexiones ricas usando la caché de líneas
        # physical.lines_summary es ["L1", "L3"]
        rich_connections = self._build_rich_connections(
            line_entries=physical.lines_summary,
            current_line_name="",
            station_transport_type=t_type
        )

        return Station(
            id=physical.id,
            original_id=physical.id.split('-')[-1] if '-' in physical.id else physical.id,
            code="",
            name=physical.name,
            latitude=physical.latitude,
            longitude=physical.longitude,
            description=physical.description,
            transport_type=t_type,
            
            # Campos de Ruta: Al ser búsqueda global, no aplican
            order=0, 
            line_code=None,
            line_name=None,
            direction=None,
            is_origin=False,
            is_destination=False,

            # Campos Extra
            station_group_code=int(extra.get('station_group_code')) if extra.get('station_group_code') else None,
            moute_id=str(extra.get('moute_id')) if extra.get('moute_id') else None,
            
            has_alerts=False,
            alerts=[],
            connections=rich_connections
        )
    
    def _map_route_stop_to_nearby_station(self, db_stop: DBRouteStop) -> NearbyStation:
        phys = db_stop.station  
        line = db_stop.line

        return NearbyStation(
            type=phys.transport_type,
            station_name=phys.name,
            physical_station_id=phys.id,
            coordinates=(phys.latitude, phys.longitude),
            distance_km=0.0,
            
            lines=[{
                "id": line.id,
                "name": line.name,
                "color": line.color or "000000"
            }],
            
            slots=None,
            mechanical=None,
            electrical=None,
            availability=None
        )

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