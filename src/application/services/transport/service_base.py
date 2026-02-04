from abc import abstractmethod
import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Callable, Any, Dict, List, Optional, Set, TypeVar
import time

from rapidfuzz import process, fuzz

from src.domain.models.common.nearby_station import NearbyStation
from src.domain.models.common.connections import Connections
from src.infrastructure.database.repositories.stations_repository import StationsRepository
from src.domain.models.common.station import Station
from src.infrastructure.mappers.line_mapper import LineMapper
from src.domain.schemas.models import DBLine, DBPhysicalStation, DBRouteStop
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
        self.cache_service = cache_service
        self.user_data_manager = user_data_manager
        self._lines_metadata_cache: Dict[str, DBLine] = {}
        self._cache_last_updated = 0

    async def _ensure_lines_cache(self):
        if self._lines_metadata_cache:
            return

        logger.info("🔄 Pre-loading lines cache for rich connections...")
        
        # Obtenemos TODAS las líneas (bus, metro, tram...)
        all_lines = await self.line_repository.get_all(transport_type=None)
        
        # CLAVE COMPUESTA: "metro-L1", "bus-V5", "bus-1"
        self._lines_metadata_cache = {
            f"{line.transport_type}-{line.code}": line 
            for line in all_lines
        }
        
        logger.info(f"✅ Lines cache loaded with {len(self._lines_metadata_cache)} unique lines.")

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

            if not line.origin or not line.destination or line.origin == line.destination:
                continue
            
            if model.extra_data and not line.category:
                line.category = model.extra_data.get('category')
            
            final_lines.append(line)

        self._enrich_with_alerts(final_lines, alerts_dict, key_attr="name")

        final_lines.sort(key=Utils.sort_lines)
        
        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_all_lines -> {len(final_lines)} lines ({elapsed:.4f}s)")
        return final_lines

    async def get_stations_by_line_code(self, transport_type: TransportType, line_code: str) -> List[Station]:
        # 1. Cargar caché de líneas (si no está cargada)
        await self._ensure_lines_cache()

        # 2. Query Principal (RouteStops)
        db_results, alerts_dict = await asyncio.gather(
            self.stations_repository.get_by_line_id(f"{transport_type.value}-{line_code}"),
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
                line_codes=physical.lines_summary,
                current_line_code=line_code,
                station_transport_type=transport_type
            )

            domain_obj = Station(
                id=physical.id,
                original_id=physical.id.split('-')[-1] if '-' in physical.id else physical.id,
                code=physical.code or "",
                name=physical.name,
                latitude=physical.latitude,
                longitude=physical.longitude,
                order=route_stop.order,
                transport_type=transport_type,
                description=physical.description,
                
                # Contexto
                line_code=line_code,
                line_name=getattr(route_stop.line, 'name', line_code),
                
                # Extra Data
                station_group_code=int(extra.get('station_group_code')) if extra.get('station_group_code') else None,
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

    def _build_rich_connections(self, line_codes: List[str], current_line_code: str, station_transport_type: TransportType) -> Optional["Connections"]:
        if not line_codes:
            return None

        rich_lines = []
        
        # Convertimos el Enum a string (ej: "metro", "bus")
        type_prefix = station_transport_type.value if hasattr(station_transport_type, 'value') else str(station_transport_type)
        
        for code in line_codes:
            if code == current_line_code:
                continue

            # BÚSQUEDA CONTEXTUAL
            lookup_key = f"{type_prefix}-{code}"
            
            line_data = self._lines_metadata_cache.get(lookup_key)
            
            if not line_data and type_prefix == 'nitbus':
                 line_data = self._lines_metadata_cache.get(f"bus-{code}")

            if line_data:
                rich_lines.append({
                    "id": line_data.id,
                    "code": line_data.code,
                    "name": line_data.name,
                    "description": line_data.description,
                    "origin": line_data.origin,
                    "destination": line_data.destination,
                    "color": line_data.color,
                    "transport_type": line_data.transport_type
                })
            else:
                rich_lines.append({
                    "id": f"unknown-{code}",
                    "code": code,
                    "name": code,
                    "color": "000000"
                })

        return {"lines": rich_lines}
    
    async def get_station_by_code(self, station_code: str, transport_type: TransportType) -> Optional[Station]:
        """
        Busca una estación de forma eficiente.
        Intenta usar la caché global si existe; si no, hace una query puntual a la DB.
        """
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

    async def get_stations_by_name(self, station_name: str, transport_type: TransportType = TransportType.METRO) -> List[Station]:
        """
        Obtiene estaciones FÍSICAS para el buscador o el mapa general.
        """
        start = time.perf_counter()
        cache_key = f"all_stations_{transport_type.value}"

        # Función interna para buscar en DB y Mapear si no hay caché
        async def fetch_and_map():
            # 1. Asegurar que tenemos metadatos de líneas para las conexiones
            await self._ensure_lines_cache()

            # 2. Obtener estaciones físicas únicas (DBPhysicalStation)
            physical_models = await self.stations_repository.get_by_transport_type(transport_type.value)
            
            # 3. Mapear a Dominio usando la caché de líneas
            return [
                self._map_physical_to_domain(p_model, transport_type) 
                for p_model in physical_models
            ]

        # Orquestación de Caché + API + Alertas
        stations_task = self._get_from_cache_or_api(
            cache_key=cache_key,
            api_call=fetch_and_map,
            cache_ttl=86400 # 24 horas (las paradas físicas no se mueven)
        )
        alerts_task = self._get_alerts_map(transport_type)

        # Ejecución paralela
        all_stations, alerts_dict = await asyncio.gather(stations_task, alerts_task)

        # Filtrado (Buscador)
        if not station_name:
            result = all_stations
        else:
            result = self.fuzzy_search(
                query=station_name, 
                items=all_stations, 
                key=lambda s: s.name, 
                threshold=75
            )

        # Enriquecimiento final con alertas en tiempo real
        self._enrich_with_alerts(result, alerts_dict, key_attr="name")

        elapsed = time.perf_counter() - start
        logger.info(f"[{self.__class__.__name__}] get_stations_by_name('{station_name}') -> {len(result)} matches ({elapsed:.4f}s)")
        return result

    async def get_nearby_stations(self, lat: float, lon: float, radius: float, transport_type: TransportType = None, limit: int = 50) -> List[NearbyStation]:
        start = time.perf_counter()
        
        # 1. Consultar Repository (Filtro en base de datos)
        # Esto nos devuelve solo lo que está cerca
        db_results = await self.stations_repository.get_nearby(
            lat=lat, 
            lon=lon, 
            radius_km=radius, 
            transport_type=transport_type,
            limit=limit
        )

        if not db_results:
            return []

        # Aseguramos caché para las conexiones ricas
        await self._ensure_lines_cache()

        # 2. Mapear resultados
        final_results = []
        for db_obj, distance in db_results:
            t_type = TransportType(db_obj.transport_type)
            station_obj = self._map_physical_to_domain(db_obj, t_type)
            
            nearby = NearbyStation(
                type=station_obj.transport_type.value,
                station_name=station_obj.name,
                station_code=station_obj.code,
                coordinates=(station_obj.latitude, station_obj.longitude),
                distance_km=distance,
                line_name=station_obj.line_name or "",
                line_code=station_obj.line_code or ""
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
            'destination', 'origin', 'color', 'extra_data'
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

    async def sync_stations(self, transport_type: TransportType, valid_lines_filter: set = None):
        """
        Sincroniza estaciones con arquitectura de 2 niveles:
        1. PhysicalStation: Infraestructura única (deduplicada por Grupo o Coordenadas).
        2. RouteStop: Evento de parada en una línea específica.
        """
        raw_stations = await self.fetch_stations()
        logger.info(f"⏳ {len(raw_stations)} {transport_type.value} stations found. Starting hybrid sync...")

        # 0. Filtro previo de líneas (si aplica)
        if valid_lines_filter:
            original_count = len(raw_stations)
            raw_stations = [
                s for s in raw_stations 
                if f"{transport_type.value}-{s.line_code}" in valid_lines_filter
            ]
            diff = original_count - len(raw_stations)
            if diff > 0:
                logger.warning(f"🧹 {transport_type.value}: Se descartaron {diff} estaciones huérfanas.")

        # --- FASE 1: Procesamiento en Memoria (Deduplicación Inteligente) ---
        
        physical_stations_map = {}
        stops_by_line = defaultdict(list)
        
        # Índice de deduplicación híbrido.
        # Guarda claves tanto de GRUPO ("group-123") como de POSICIÓN ((lat, lon))
        # Valor: El 'clean_id' maestro que debe usarse.
        dedup_lookup = {} 

        for raw in raw_stations:
            # A. EXTRACCIÓN DE DATOS (Necesaria para ver si hay group_code)
            # Extraemos extra_data usando tu método auxiliar
            extra = self._extract_extra_data(raw, {'id', 'original_id', 'code', 'name', 'lat', 'lon'})
            
            # Sanitización de JSON inmediata (Convertir Enums a Strings para evitar crasheos)
            t_type_str = transport_type.value if hasattr(transport_type, 'value') else str(transport_type)
            if extra:
                for k, v in extra.items():
                    if isinstance(v, TransportType):
                        extra[k] = v.value

            # Intentamos obtener el código de grupo (común en Metro/Tram)
            group_code = None
            if extra and 'station_group_code' in extra:
                group_code = extra['station_group_code']

            # B. LÓGICA DE DEDUPLICACIÓN (Buscamos si la estación ya existe)
            clean_id = None
            
            # Prioridad 1: Por Código de Grupo (Lógica estricta)
            if group_code:
                group_key = f"group-{group_code}"
                if group_key in dedup_lookup:
                    clean_id = dedup_lookup[group_key]

            # Prioridad 2: Por Coordenadas (Si no hay grupo o no se encontró)
            if not clean_id:
                # Redondeo a 5 decimales (~1.1 metros) para agrupar postes idénticos
                lat_rounded = round(raw.latitude, 5)
                lon_rounded = round(raw.longitude, 5)
                coord_key = (lat_rounded, lon_rounded)
                
                if coord_key in dedup_lookup:
                    clean_id = dedup_lookup[coord_key]

            # Si sigue siendo None, es una estación NUEVA
            if not clean_id:
                try:
                    number_part = str(int(raw.id)) # "00055" -> "55"
                except (ValueError, TypeError):
                    number_part = str(raw.id)

                # Prefijo para Namespacing (evitar colisión Bus 33 vs Metro 33)
                # Unificamos 'nitbus' bajo 'bus' para compartir iconos
                prefix = t_type_str
                if prefix == 'nitbus': prefix = 'bus'
                
                clean_id = f"{prefix}-{number_part}"
                
                # REGISTRAR EN EL LOOKUP (Para que las siguientes hermanas la encuentren)
                if group_code:
                    dedup_lookup[f"group-{group_code}"] = clean_id
                
                # Siempre registramos por coordenadas también
                dedup_lookup[(round(raw.latitude, 5), round(raw.longitude, 5))] = clean_id

            # C. CONSTRUCCIÓN DE ESTACIÓN FÍSICA
            if clean_id not in physical_stations_map:
                physical_stations_map[clean_id] = {
                    "id": clean_id,
                    "code": str(raw.code) if raw.code else None,
                    "name": raw.name,
                    "description": raw.description,
                    "lat": raw.latitude, # Usamos la latitud real de la primera ocurrencia
                    "lon": raw.longitude,
                    "municipality": getattr(raw, 'municipality', None),
                    "transport_type": t_type_str,
                    "extra_data": extra,
                    "lines_set": set()
                }
            
            # Añadimos la línea al set (Merge automático de líneas L1, L2, etc.)
            physical_stations_map[clean_id]["lines_set"].add(raw.line_code)

            # D. PREPARACIÓN DE ROUTE STOPS (Datos de ruta)
            # Guardamos tupla (raw, clean_id) para no modificar el objeto original
            line_db_id = f"{transport_type.value}-{raw.line_code}"
            direction = getattr(raw, 'direction', 'única')
            
            stops_by_line[(line_db_id, direction)].append((raw, clean_id))

        # --- FASE 2: Lógica de Ruta (Orden y Origen/Destino) ---
        
        route_stops_buffer = []

        for (line_id, direction), stops_tuples in stops_by_line.items():
            # Ordenamos por el campo 'order' del objeto raw (posición 0 de la tupla)
            sorted_tuples = sorted(stops_tuples, key=lambda x: x[0].order)
            total_stops = len(sorted_tuples)
            
            for index, (stop, s_clean_id) in enumerate(sorted_tuples):
                route_stops_buffer.append(DBRouteStop(
                    line_id=line_id,
                    station_id=s_clean_id, # Usamos el ID físico deduplicado
                    order=stop.order,
                    direction=direction,
                    is_origin=(index == 0),
                    is_destination=(index == total_stops - 1)
                ))

        # --- FASE 3: Persistencia Robusta (UPSERT) ---
        
        async with async_session_factory() as session:
            try:
                # 3.1 Guardar Estaciones Físicas (UPSERT por lotes)
                if physical_stations_map:
                    stations_data = []
                    for p_data in physical_stations_map.values():
                        stations_data.append({
                            "id": p_data["id"],
                            "code": p_data["code"],
                            "name": p_data["name"],
                            "description": p_data["description"],
                            "latitude": p_data["lat"],
                            "longitude": p_data["lon"],
                            "municipality": p_data["municipality"],
                            "transport_type": p_data["transport_type"],
                            "extra_data": p_data["extra_data"],
                            "lines_summary": sorted(list(p_data["lines_set"])),
                            "updated_at": datetime.utcnow()
                        })

                    logger.info(f"📍 Upserting {len(stations_data)} physical stations...")

                    # DEFINIMOS TAMAÑO DE LOTE (1000 * 11 cols = 11.000 params < 32.767)
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
                    
                    # Flush intermedio para asegurar que las FK existen
                    await session.flush()

                # 3.2 Guardar Route Stops (Batching manual recomendado para volúmenes altos)
                if route_stops_buffer:
                    logger.info(f"🚏 Inserting {len(route_stops_buffer)} route stops...")
                    
                    # Aunque SQLAlchemy suele gestionar esto, para evitar problemas con drivers
                    # asíncronos en inserciones masivas (>10k), mejor lo troceamos también.
                    BATCH_SIZE_STOPS = 2000 # Menos columnas, podemos meter más filas
                    
                    for i in range(0, len(route_stops_buffer), BATCH_SIZE_STOPS):
                        chunk_stops = route_stops_buffer[i : i + BATCH_SIZE_STOPS]
                        session.add_all(chunk_stops)
                        await session.flush() # Flush por cada lote para liberar memoria
                
                await session.commit()
                logger.info(f"✅ {transport_type.value} Sync completed successfully.")

            except Exception as e:
                logger.error(f"❌ Error syncing stations: {e}")
                await session.rollback()
                raise e

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
        for item in items:
            item_alerts = []
            seen_ids = set()
            
            is_station_item = hasattr(item, 'line_name') and item.line_name is not None

            primary_key = getattr(item, key_attr, "")
            if primary_key in alerts_map:
                for alert in alerts_map[primary_key]:
                    if alert.id not in seen_ids:
                        item_alerts.append(alert)
                        seen_ids.add(alert.id)
            
            if is_station_item:
                line_key = item.line_name
                if line_key in alerts_map:
                    for alert in alerts_map[line_key]:
                        if alert.id in seen_ids:
                            continue
                        
                        targets_specific_stations = any(e.station_name for e in alert.affected_entities)
                        targets_me = any(e.station_name == item.name for e in alert.affected_entities)

                        if targets_specific_stations and not targets_me:
                            continue

                        item_alerts.append(alert)
                        seen_ids.add(alert.id)

            item.alerts = item_alerts
            item.has_alerts = len(item_alerts) > 0

    async def _get_alerts_map(self, transport_type: TransportType) -> Dict[str, List[Alert]]:
        cache_key = f"{transport_type.value}_alerts_map"
        
        cached = await self.cache_service.get(cache_key)
        if cached: return cached

        try:
            raw_alerts = await self.fetch_alerts()
            if not raw_alerts:
                return {}

            result = defaultdict(list)
            
            for alert in raw_alerts:
                await self.user_data_manager.register_alert(transport_type, alert)
                
                entities = alert.affected_entities or []
                
                for entity in entities:
                    if entity.station_name:
                         result[entity.station_name].append(alert)
                         
                    if entity.line_name:
                         result[entity.line_name].append(alert)
            
            alerts_dict = dict(result)
            await self.cache_service.set(cache_key, alerts_dict, ttl=3600)
            return alerts_dict

        except Exception as e:
            logger.error(f"Error alerts map: {e}")
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
        """
        Mapea una estación física (sin contexto de línea específica) al dominio.
        Los campos de ruta (order, line_code) se quedan vacíos o genéricos.
        """
        extra = physical.extra_data or {}
        
        # Construimos conexiones ricas usando la caché de líneas
        # physical.lines_summary es ["L1", "L3"]
        rich_connections = self._build_rich_connections(
            line_codes=physical.lines_summary,
            current_line_code="", # No estamos en ninguna línea específica
            station_transport_type=t_type
        )

        return Station(
            id=physical.id,
            original_id=physical.id.split('-')[-1] if '-' in physical.id else physical.id,
            code=physical.code or "",
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