import io
import asyncio
import requests
import zipfile
import pandas as pd
from typing import List, Optional

from src.domain.models.common.station import Station
from src.domain.models.common.line import Line

class AmbApiService:
    GTFS_URL = "https://www.ambmobilitat.cat/OpenData/google_transit.zip"

    # ==========================================
    # MÉTODOS PRIVADOS (SÍNCRONOS / BLOQUEANTES)
    # ==========================================
    
    @staticmethod
    def _download_gtfs_content_sync() -> Optional[bytes]:
        """Descarga bloqueante (se ejecutará en un hilo)."""
        print(f"⬇️ Descargando GTFS desde {AmbApiService.GTFS_URL}...")
        try:
            response = requests.get(AmbApiService.GTFS_URL, timeout=60) # Timeout recomendado
            response.raise_for_status()
            return response.content
        except Exception as e:
            print(f"❌ Error descargando GTFS: {e}")
            return None

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

    @staticmethod
    def _get_lines_sync() -> List[Line]:
        """Lógica pesada de procesamiento de líneas (CPU Bound)."""
        content = AmbApiService._download_gtfs_content_sync()
        if not content: return []

        print("📦 Procesando Rutas (Lines)...")
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            with z.open('routes.txt') as f:
                df_routes = pd.read_csv(f, dtype={'route_id': str, 'agency_id': str})
            with z.open('agency.txt') as f:
                df_agency = pd.read_csv(f, dtype={'agency_id': str})

        df_merged = pd.merge(df_routes, df_agency[['agency_id', 'agency_name']], on='agency_id', how='left')
        df_merged.fillna({'route_short_name': '', 'route_long_name': '', 'agency_name': ''}, inplace=True)
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

            t_type = AmbApiService.map_transport_type(int(row['route_type']), row['agency_name'])

            line_obj = Line(
                id=row['route_id'],
                original_id=row['route_id'],
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

        print(f"✅ {len(lines_list)} líneas obtenidas.")
        return lines_list

    @staticmethod
    def _get_stations_sync() -> List[Station]:
        """Lógica pesada de procesamiento de estaciones (CPU Bound)."""
        content = AmbApiService._download_gtfs_content_sync()
        if not content: return []

        print("📦 Procesando Estaciones (Stations)...")
        
        lines_context = {}
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            with z.open('routes.txt') as f:
                df_routes = pd.read_csv(f, dtype={'route_id': str, 'agency_id': str})
            with z.open('agency.txt') as f:
                df_agency = pd.read_csv(f, dtype={'agency_id': str})
            
            df_ctx = pd.merge(df_routes, df_agency[['agency_id', 'agency_name']], on='agency_id', how='left')
            
            for _, row in df_ctx.iterrows():
                short = str(row['route_short_name']) if pd.notna(row['route_short_name']) else ""
                long_n = str(row['route_long_name']) if pd.notna(row['route_long_name']) else ""
                t_type = AmbApiService.map_transport_type(int(row['route_type']), str(row['agency_name']))
                
                lines_context[str(row['route_id'])] = {
                    "code": short if short else long_n[:4],
                    "name": long_n if long_n else short,
                    "type": t_type
                }

            # Trips
            cols_trips = ['route_id', 'trip_id', 'direction_id']
            header_trips = pd.read_csv(z.open('trips.txt'), nrows=0).columns.tolist()
            if 'trip_headsign' in header_trips: cols_trips.append('trip_headsign')

            with z.open('trips.txt') as f:
                df_trips = pd.read_csv(f, usecols=cols_trips, dtype=str)
            
            if 'trip_headsign' not in df_trips.columns: df_trips['trip_headsign'] = ""
            df_trips['trip_headsign'] = df_trips['trip_headsign'].fillna("")

            # Stop Times
            with z.open('stop_times.txt') as f:
                df_stop_times = pd.read_csv(f, usecols=['trip_id', 'stop_id', 'stop_sequence'], 
                                            dtype={'trip_id': str, 'stop_id': str, 'stop_sequence': int})
            
            # Stops
            cols_stops = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon']
            header_stops = pd.read_csv(z.open('stops.txt'), nrows=0).columns.tolist()
            if 'stop_code' in header_stops: cols_stops.append('stop_code')
            if 'stop_desc' in header_stops: cols_stops.append('stop_desc')

            with z.open('stops.txt') as f:
                df_stops = pd.read_csv(f, usecols=cols_stops, dtype={'stop_id': str})

        print("🔄 Calculando viajes canónicos y destinos reales...")
        
        trip_counts = df_stop_times.groupby('trip_id').size().reset_index(name='count')
        trips_with_counts = pd.merge(df_trips, trip_counts, on='trip_id')
        best_trips = trips_with_counts.sort_values('count', ascending=False).drop_duplicates(['route_id', 'direction_id'])
        
        relevant_stop_times = pd.merge(df_stop_times, best_trips, on='trip_id')
        full_data = pd.merge(relevant_stop_times, df_stops, on='stop_id', how='left')
        full_data.sort_values(['route_id', 'direction_id', 'stop_sequence'], inplace=True)

        trip_destinations = {}
        for trip_id, group in full_data.groupby('trip_id'):
            headsign = str(group.iloc[0]['trip_headsign']).strip()
            if headsign and len(headsign) > 2:
                trip_destinations[trip_id] = headsign
            else:
                trip_destinations[trip_id] = group.iloc[-1]['stop_name']

        print("🔄 Generando lista de objetos Station...")
        all_stations: List[Station] = []

        for _, row in full_data.iterrows():
            r_id = str(row['route_id'])
            t_id = str(row['trip_id'])
            
            ctx = lines_context.get(r_id)
            if not ctx: continue

            real_direction_name = trip_destinations.get(t_id, str(row['direction_id']))
            code_val = row['stop_code'] if 'stop_code' in row and pd.notna(row['stop_code']) else row['stop_id']
            desc_val = row['stop_desc'] if 'stop_desc' in row and pd.notna(row['stop_desc']) else None

            station = Station(
                id=str(row['stop_id']),
                original_id=str(row['stop_id']),
                code=str(code_val),
                name=row['stop_name'],
                latitude=float(row['stop_lat']),
                longitude=float(row['stop_lon']),
                order=int(row['stop_sequence']),
                transport_type=ctx["type"],
                description=desc_val,
                line_code=ctx["code"],
                line_name=ctx["name"],
                direction=real_direction_name, 
                moute_id=r_id,
                has_alerts=False,
                alerts=[],
                connections=None
            )
            all_stations.append(station)

        print(f"✅ {len(all_stations)} estaciones procesadas correctamente.")
        return all_stations

    # ==========================================
    # MÉTODOS PÚBLICOS (ASYNC WRAPPERS)
    # ==========================================

    @staticmethod
    async def get_lines() -> List[Line]:
        """Versión Async que ejecuta el procesamiento en un hilo separado."""
        return await asyncio.to_thread(AmbApiService._get_lines_sync)

    @staticmethod
    async def get_stations() -> List[Station]:
        """Versión Async que ejecuta el procesamiento en un hilo separado."""
        return await asyncio.to_thread(AmbApiService._get_stations_sync)