import io
import asyncio
import logging
import requests
import zipfile
import pandas as pd
import pytz
from typing import List, Dict
from datetime import datetime

from src.domain.models.common.station import Station
from src.domain.models.common.line import Line, TransportType
from src.domain.models.common.line_route import LineRoute, NextTrip

from src.core.logger import logger

# ==========================================
# SINGLETON STORE (Memoria RAM)
# ==========================================
class AmbGtfsStore:
    """
    Clase auxiliar Singleton para mantener los DataFrames en memoria.
    Evita descargar el ZIP de 300MB en cada petición.
    """
    GTFS_URL = "https://www.ambmobilitat.cat/OpenData/google_transit.zip"
    _instance = None
    
    data: Dict[str, pd.DataFrame] = {}
    is_loaded: bool = False
    is_loading: bool = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AmbGtfsStore, cls).__new__(cls)
        return cls._instance

    @classmethod
    def load_data(cls):
        """Descarga y procesa el GTFS una sola vez."""
        if cls.is_loading: return
        cls.is_loading = True
        logger.info("⬇️ [AmbGtfsStore] Iniciando carga/actualización de GTFS...")

        try:
            response = requests.get(cls.GTFS_URL, timeout=120)
            response.raise_for_status()
            content = io.BytesIO(response.content)

            new_data = {}
            with zipfile.ZipFile(content) as z:
                # 1. Cargas básicas
                new_data['agency'] = pd.read_csv(z.open('agency.txt'), dtype=str)
                new_data['routes'] = pd.read_csv(z.open('routes.txt'), dtype=str)
                new_data['trips'] = pd.read_csv(z.open('trips.txt'), dtype=str)
                
                # Stops: Aseguramos todas las columnas necesarias
                cols_stops = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon']
                header_stops = pd.read_csv(z.open('stops.txt'), nrows=0).columns.tolist()
                if 'stop_code' in header_stops: cols_stops.append('stop_code')
                if 'stop_desc' in header_stops: cols_stops.append('stop_desc')
                new_data['stops'] = pd.read_csv(z.open('stops.txt'), usecols=cols_stops, dtype={'stop_id': str, 'stop_code': str})

                # 2. Stop Times (Optimizado para memoria y velocidad)
                # Solo cargamos lo vital
                logger.info("📦 [AmbGtfsStore] Procesando Stop Times...")
                df_st = pd.read_csv(
                    z.open('stop_times.txt'),
                    usecols=['trip_id', 'stop_id', 'arrival_time', 'stop_sequence'],
                    dtype={'trip_id': str, 'stop_id': str, 'stop_sequence': int}
                )
                
                # Convertimos hora a segundos UNA VEZ aquí para siempre
                def fast_time_convert(t):
                    try:
                        parts = t.split(':')
                        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                    except: return 0

                df_st['arrival_seconds'] = df_st['arrival_time'].apply(fast_time_convert).astype('int32')
                # Eliminamos arrival_time texto para ahorrar RAM
                df_st.drop(columns=['arrival_time'], inplace=True)
                new_data['stop_times'] = df_st.sort_values('arrival_seconds')

                # 3. Pre-calculo de Joins comunes (Trips + Routes)
                new_data['trips_routes'] = pd.merge(new_data['trips'], new_data['routes'], on='route_id', how='left')

            cls.data = new_data
            cls.is_loaded = True
            logger.info("✅ [AmbGtfsStore] Datos GTFS cargados en memoria.")

        except Exception as e:
            logger.error(f"❌ [AmbGtfsStore] Error cargando GTFS: {e}")
        finally:
            cls.is_loading = False


# ==========================================
# SERVICIO PRINCIPAL
# ==========================================
class AmbApiService:
    GTFS_URL = AmbGtfsStore.GTFS_URL
    store = AmbGtfsStore()

    @classmethod
    async def initialize(cls):
        """Llamar a esto al iniciar la aplicación para cargar datos."""
        if not cls.store.is_loaded:
            await asyncio.to_thread(cls.store.load_data)

    # ==========================================
    # UTILIDADES ESTÁTICAS (Mantenidas)
    # ==========================================

    @staticmethod
    def map_transport_type(route_type: int, agency_name: str) -> str:
        agency_upper = str(agency_name).upper()
        if route_type == 0: return "tram"
        if route_type == 1: return "metro"
        if route_type == 2:
            if "RENFE" in agency_upper or "RODALIES" in agency_upper: return "rodalies"
            if "FGC" in agency_upper or "FERROCARRILS" in agency_upper: return "fgc"
            return "rodalies"
        if route_type == 3: return "bus"
        if route_type in [11, 12]: return "funicular"
        return "bus"

    # ==========================================
    # MÉTODOS DE NEGOCIO (Refactorizados a Memoria)
    # ==========================================

    @staticmethod
    def _get_lines_sync() -> List[Line]:
        """Obtiene las líneas usando los datos en memoria."""
        data = AmbGtfsStore.data
        if not data: return []

        logger.info("📦 Procesando Rutas (Lines) desde Memoria...")
        df_routes = data['routes']
        df_agency = data['agency']

        df_merged = pd.merge(df_routes, df_agency[['agency_id', 'agency_name']], on='agency_id', how='left')
        df_merged.fillna({'route_short_name': '', 'route_long_name': '', 'agency_name': ''}, inplace=True)
        # Fix color nan
        if 'route_color' not in df_merged.columns: df_merged['route_color'] = '000000'
        df_merged['route_color'] = df_merged['route_color'].fillna('000000')

        lines_list: List[Line] = []

        for _, row in df_merged.iterrows():
            short_name = str(row['route_short_name'])
            long_name = str(row['route_long_name'])
            
            code = short_name if short_name else long_name[:4].strip()
            display_name = long_name if long_name else short_name

            if "Turístic" in display_name: continue
            
            origin, dest = "", ""
            if " - " in display_name:
                parts = display_name.split(" - ")
                if len(parts) >= 2: origin, dest = parts[0], parts[-1]
            elif "-" in display_name:
                parts = display_name.split("-")
                if len(parts) >= 2: origin, dest = parts[0], parts[-1]

            color_hex = str(row['route_color'])
            if not color_hex.startswith('#'): color_hex = f"#{color_hex}"

            r_type_val = int(row['route_type']) if pd.notna(row['route_type']) else 3
            t_type = AmbApiService.map_transport_type(r_type_val, row['agency_name'])

            line_obj = Line(
                id=str(row['route_id']),
                original_id=str(row['route_id']),
                code=code,
                name=short_name if short_name else display_name,
                description=display_name,
                transport_type=t_type,
                color=color_hex,
                origin=origin,
                destination=dest,
                has_alerts=False,
                extra_data={
                    "agency_name": row['agency_name'],
                    "agency_id": row['agency_id'],
                    "gtfs_route_type": row['route_type']
                }
            )            
            lines_list.append(line_obj)

        logger.info(f"✅ {len(lines_list)} líneas obtenidas.")
        return lines_list

    @staticmethod
    def _get_stations_sync() -> List[Station]:
        """Procesa estaciones usando los datos en memoria."""
        data = AmbGtfsStore.data
        if not data: return []

        logger.info("📦 Procesando Estaciones (Stations) desde Memoria...")
        
        # 1. Contexto de Líneas
        df_routes = data['routes']
        df_agency = data['agency']
        df_ctx = pd.merge(df_routes, df_agency[['agency_id', 'agency_name']], on='agency_id', how='left')
        
        lines_context = {}
        for _, row in df_ctx.iterrows():
            short = str(row['route_short_name']) if pd.notna(row['route_short_name']) else ""
            long_n = str(row['route_long_name']) if pd.notna(row['route_long_name']) else ""
            
            r_type_val = int(row['route_type']) if pd.notna(row['route_type']) else 3
            t_type = AmbApiService.map_transport_type(r_type_val, str(row['agency_name']))
            
            lines_context[str(row['route_id'])] = {
                "code": short if short else long_n[:4],
                "name": long_n if long_n else short,
                "type": t_type
            }

        # 2. Datos necesarios
        df_trips = data['trips']
        df_stop_times = data['stop_times'] # Ya tiene arrival_seconds, pero aquí necesitamos agrupar
        df_stops = data['stops']

        # Asegurar columna headsign si no existe
        if 'trip_headsign' not in df_trips.columns:
            df_trips = df_trips.copy()
            df_trips['trip_headsign'] = ""
        
        # 3. Lógica Canónica (Tu lógica original optimizada)
        logger.info("🔄 Calculando viajes canónicos...")
        trip_counts = df_stop_times.groupby('trip_id').size().reset_index(name='count')
        trips_with_counts = pd.merge(df_trips, trip_counts, on='trip_id')
        best_trips = trips_with_counts.sort_values('count', ascending=False).drop_duplicates(['route_id', 'direction_id'])
        
        relevant_stop_times = pd.merge(df_stop_times, best_trips, on='trip_id')
        full_data = pd.merge(relevant_stop_times, df_stops, on='stop_id', how='left')
        full_data.sort_values(['route_id', 'direction_id', 'stop_sequence'], inplace=True)

        # 4. Mapa de Destinos
        trip_destinations = {}
        # Optimizacion: groupby es lento en loops, iteramos sobre el df ordenado
        # Pero mantenemos tu lógica por seguridad
        for trip_id, group in full_data.groupby('trip_id'):
            headsign = str(group.iloc[0]['trip_headsign']).strip()
            if headsign and len(headsign) > 2 and headsign != "nan":
                trip_destinations[trip_id] = headsign
            else:
                trip_destinations[trip_id] = group.iloc[-1]['stop_name']

        # 5. Generación de Objetos
        all_stations: List[Station] = []
        for _, row in full_data.iterrows():
            r_id = str(row['route_id'])
            t_id = str(row['trip_id'])
            
            ctx = lines_context.get(r_id)
            if not ctx: continue

            real_dir = trip_destinations.get(t_id, str(row['direction_id']))
            code_val = row['stop_code'] if 'stop_code' in row and pd.notna(row['stop_code']) else row['stop_id']
            desc_val = row['stop_desc'] if 'stop_desc' in row and pd.notna(row['stop_desc']) else None

            station = Station(
                id=str(row['stop_id']),
                original_id=str(row['stop_id']),
                code=str(code_val),
                name=str(row['stop_name']),
                latitude=float(row['stop_lat']),
                longitude=float(row['stop_lon']),
                order=int(row['stop_sequence']),
                transport_type=ctx["type"],
                description=desc_val,
                line_code=ctx["code"],
                line_name=ctx["name"],
                direction=real_dir, 
                moute_id=r_id,
                has_alerts=False,
                alerts=[],
                connections=None
            )
            all_stations.append(station)

        logger.info(f"✅ {len(all_stations)} estaciones procesadas correctamente.")
        return all_stations

    @staticmethod
    def _get_next_arrivals_sync(stop_code: str, max_results: int = 3) -> List[LineRoute]:
        """
        NUEVO MÉTODO: Busca los próximos horarios planificados para una parada.
        Devuelve arrival_time como TIMESTAMP EXACTO (Unix seconds).
        Utiliza el Store en memoria para respuesta rápida.
        """
        data = AmbGtfsStore.data
        if not data:
            logger.warning("⚠️ Datos GTFS no cargados. Llama a initialize() primero.")
            return []

        # 1. Configuración de Tiempo base (Medianoche de hoy)
        tz = pytz.timezone('Europe/Madrid')
        now = datetime.now(tz)
        
        # Obtenemos el timestamp de las 00:00:00 de HOY
        today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        midnight_timestamp = today_midnight.timestamp()

        # Segundos actuales desde medianoche para el filtrado
        current_seconds = now.hour * 3600 + now.minute * 60 + now.second

        # 2. Encontrar Stop IDs
        df_stops = data['stops']
        mask_stop = (df_stops['stop_code'] == stop_code) | (df_stops['stop_id'] == stop_code)
        target_stops = df_stops[mask_stop]
        
        if target_stops.empty:
            logger.error(f"❌ Parada {stop_code} no encontrada.")
            return []
        
        target_ids = target_stops['stop_id'].unique()

        # 3. Filtrar Stop Times
        df_st = data['stop_times']
        
        # Buscamos viajes desde 'ahora' hasta 'ahora + 2 horas'
        window = 7200 # 2 horas
        
        mask_times = (
            (df_st['stop_id'].isin(target_ids)) &
            (df_st['arrival_seconds'] >= current_seconds - 300) & # -5 min margen
            (df_st['arrival_seconds'] <= current_seconds + window)
        )
        candidates = df_st[mask_times].copy()
        
        if candidates.empty:
            return []

        # 4. Join con Trips+Routes+Agency
        df_trips_routes = data['trips_routes']
        df_full = pd.merge(candidates, df_trips_routes, on='trip_id', how='left')
        
        df_agency = data['agency']
        if 'agency_id' in df_full.columns:
             df_full = pd.merge(df_full, df_agency[['agency_id', 'agency_name']], on='agency_id', how='left')
        else:
             df_full['agency_name'] = "AMB"

        # 5. Agrupar y Formatear
        df_full.sort_values('arrival_seconds', inplace=True)
        
        routes_list: List[LineRoute] = []
        
        group_cols = ['route_id', 'trip_headsign', 'route_short_name', 'route_long_name', 'route_color', 'route_type', 'agency_name']
        
        for keys, group in df_full.groupby(group_cols, sort=False):
            r_id, headsign, r_short, r_long, r_color, r_type, agency = keys
            
            # Top N viajes
            next_trips_df = group.head(max_results)
            
            next_trips_objs = []
            for _, trip in next_trips_df.iterrows():
                arr_sec_from_midnight = int(trip['arrival_seconds'])
                
                # CÁLCULO DEL TIMESTAMP EXACTO
                # Medianoche (Unix) + Segundos del GTFS = Timestamp Real de llegada
                # Nota: Si el GTFS dice 25:00:00 (90000s), esto calculará correctamente
                # la hora para el día siguiente.
                exact_arrival_ts = int(midnight_timestamp + arr_sec_from_midnight)
                
                next_trips_objs.append(NextTrip(
                    id=str(trip['trip_id']),
                    arrival_time=exact_arrival_ts
                ))

            # Datos visuales
            r_short = str(r_short) if pd.notna(r_short) else ""
            r_long = str(r_long) if pd.notna(r_long) else ""
            display_name = r_short if r_short else r_long
            
            color_hex = str(r_color)
            if not color_hex.startswith('#'): color_hex = f"#{color_hex}"
            if "nan" in color_hex: color_hex = "#000000"
            
            t_type_val = int(r_type) if pd.notna(r_type) else 3
            t_type_str = AmbApiService.map_transport_type(t_type_val, str(agency))
            
            try:
                t_type_enum = TransportType(t_type_str)
            except:
                t_type_enum = TransportType.BUS

            headsign_str = str(headsign).strip()
            if not headsign_str or headsign_str == "nan":
                 headsign_str = "Destino desconocido"

            routes_list.append(LineRoute(
                route_id=str(r_id),
                line_id=str(r_id),
                line_code=display_name,
                line_name=r_long,
                line_type=t_type_enum,
                color=color_hex,
                destination=headsign_str,
                next_trips=next_trips_objs,
                name_with_emoji=f"{t_type_enum.value} {display_name}"
            ))

        return routes_list

    # ==========================================
    # MÉTODOS PÚBLICOS (ASYNC WRAPPERS)
    # ==========================================

    @staticmethod
    async def get_lines() -> List[Line]:
        """Versión Async que usa el Store en memoria."""
        if not AmbGtfsStore.is_loaded:
             # Fallback: si no se ha cargado, cargar ahora (bloqueante la primera vez)
             await asyncio.to_thread(AmbGtfsStore.load_data)
        return await asyncio.to_thread(AmbApiService._get_lines_sync)

    @staticmethod
    async def get_stations() -> List[Station]:
        """Versión Async que usa el Store en memoria."""
        if not AmbGtfsStore.is_loaded:
             await asyncio.to_thread(AmbGtfsStore.load_data)
        return await asyncio.to_thread(AmbApiService._get_stations_sync)

    @staticmethod
    async def get_next_arrivals(stop_id: str) -> List[LineRoute]:
        """
        Obtiene los próximos horarios para una parada.
        """
        if not AmbGtfsStore.is_loaded:
             await asyncio.to_thread(AmbGtfsStore.load_data)
        return await asyncio.to_thread(AmbApiService._get_next_arrivals_sync, stop_id)