import math
import time
from typing import List, Optional, Tuple, Dict
from src.domain.models.common.station import Station
from src.domain.models.common.location import Location
from src.domain.models.bicing.bicing_station import BicingStation
from src.core.logger import logger

class DistanceHelper:
    EARTH_RADIUS_KM = 6371.0  # Average Earth radius in kilometers

    @staticmethod
    def bounding_box(lat: float, lon: float, radius_km: float) -> Tuple[float, float, float, float]:
        """Returns min_lat, max_lat, min_lon, max_lon for a given point and radius in km."""
        from math import radians, cos
        delta_lat = radius_km / 111
        delta_lon = radius_km / (111 * cos(radians(lat)))
        return lat - delta_lat, lat + delta_lat, lon - delta_lon, lon + delta_lon

    @staticmethod
    def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculates the great-circle distance between two points on Earth."""
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = (math.sin(delta_phi / 2) ** 2 +
             math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return DistanceHelper.EARTH_RADIUS_KM * c

    @staticmethod
    def format_distance(distance_km: float) -> str:
        if distance_km < 1:
            return f"{int(distance_km * 1000)}m"
        else:
            return f"{distance_km:.1f}km"
        
    @staticmethod
    def build_stops_list(
        stations: List[Station],
        bicing_stations: List[BicingStation],
        user_location: Optional[Location] = None,
        results_to_return: int = 50,
        max_distance_km: float = 1000
    ) -> List[Dict]:
        start = time.perf_counter()
        stops = []

        if user_location and results_to_return == 50:
            results_to_return = 10

        if user_location:
            min_lat, max_lat, min_lon, max_lon = DistanceHelper.bounding_box(
                user_location.latitude, user_location.longitude, max_distance_km
            )
        else:
            min_lat = max_lat = min_lon = max_lon = None

        def within_bbox(lat, lon):
            if user_location is None:
                return True
            
            if lat is None or lon is None:
                return False

            try:
                lat_float = float(lat)
                lon_float = float(lon)
                
                return min_lat <= lat_float <= max_lat and min_lon <= lon_float <= max_lon
            except ValueError:
                return False
            
        for s in stations:
            if not within_bbox(s.latitude, s.longitude):
                continue

            distance_km = DistanceHelper.haversine_distance(
                s.latitude, s.longitude, user_location.latitude, user_location.longitude
            ) if user_location else None

            if distance_km is not None and distance_km > max_distance_km:
                continue

            stops.append({
                "type": s.transport_type.value,
                "line_name": s.line_name,
                "line_name_with_emoji": '',
                "line_code": s.line_code,
                "station_name": s.name,
                "station_code": s.code,
                "coordinates": (s.latitude, s.longitude),
                "distance_km": distance_km
            })

        for b in bicing_stations:
            if not within_bbox(b.latitude, b.longitude):
                continue
            distance_km = DistanceHelper.haversine_distance(
                b.latitude, b.longitude, user_location.latitude, user_location.longitude
            ) if user_location else None
            if distance_km is not None and distance_km > max_distance_km:
                continue
            stops.append({
                "type": "bicing",
                "line_name": '',
                "line_name_with_emoji": '',
                "station_name": b.streetName,
                "station_code": b.id,
                "coordinates": (b.latitude, b.longitude),
                "slots": b.slots,
                "mechanical": b.mechanical_bikes,
                "electrical": b.electrical_bikes,
                "availability": b.disponibilidad,
                "distance_km": distance_km
            })

        stops.sort(key=lambda x: (x["distance_km"] is None, x["distance_km"]))
        elapsed = time.perf_counter() - start
        logger.info(f"[DistanceHelper] build_stops_list ejecutado en {elapsed:.4f} s | {len(stops)} stops encontrados")
        return stops[:results_to_return]

    '''
    @staticmethod
    def build_stops_list(
        metro_stations: List[MetroStation],
        bus_stops: List[BusStop],
        tram_stops: List[TramStation],
        rodalies_stations: List[RodaliesStation],
        bicing_stations: List[BicingStation],
        fgc_stations: List[FgcStation],
        user_location: Optional[Location] = None,
        results_to_return: int = 50,
        max_distance_km: float = 1000
    ) -> List[Dict]:
        start = time.perf_counter()
        stops = []

        if user_location and results_to_return == 50:
            results_to_return = 10

        # Bounding box para filtrar paradas fuera del radio
        if user_location:
            min_lat, max_lat, min_lon, max_lon = DistanceHelper.bounding_box(
                user_location.latitude, user_location.longitude, max_distance_km
            )
        else:
            min_lat = max_lat = min_lon = max_lon = None

        def within_bbox(lat, lon):
            if user_location is None:
                return True
            
            if lat is None or lon is None:
                return False

            try:
                lat_float = float(lat)
                lon_float = float(lon)
                
                return min_lat <= lat_float <= max_lat and min_lon <= lon_float <= max_lon
            except ValueError:
                return False

        # --- Procesa todas las listas ---
        for m in metro_stations:
            if not within_bbox(m.latitude, m.longitude):
                continue
            distance_km = DistanceHelper.haversine_distance(
                m.latitude, m.longitude, user_location.latitude, user_location.longitude
            ) if user_location else None
            if distance_km is not None and distance_km > max_distance_km:
                continue
            stops.append({
                "type": "metro",
                "line_name": m.line_name,
                "line_name_with_emoji": m.line_name_with_emoji,
                "line_code": m.line_code,
                "station_name": m.name,
                "station_code": m.code,
                "coordinates": (m.latitude, m.longitude),
                "distance_km": distance_km
            })

        for t in tram_stops + rodalies_stations + fgc_stations:
            if not within_bbox(t.latitude, t.longitude):
                continue
            distance_km = DistanceHelper.haversine_distance(
                t.latitude, t.longitude, user_location.latitude, user_location.longitude
            ) if user_location else None
            if distance_km is not None and distance_km > max_distance_km:
                continue
            stops.append({
                "type": "tram" if isinstance(t, TramStation) else "rodalies" if isinstance(t, RodaliesStation) else "fgc",                
                "line_name": t.line_name,
                "line_name_with_emoji": t.line_name_with_emoji,
                "line_code": t.line_code,
                "station_name": t.name,
                "station_code": t.code,
                "coordinates": (t.latitude, t.longitude),
                "distance_km": distance_km
            })

        for b in bicing_stations:
            if not within_bbox(b.latitude, b.longitude):
                continue
            distance_km = DistanceHelper.haversine_distance(
                b.latitude, b.longitude, user_location.latitude, user_location.longitude
            ) if user_location else None
            if distance_km is not None and distance_km > max_distance_km:
                continue
            stops.append({
                "type": "bicing",
                "line_name": '',
                "line_name_with_emoji": '',
                "station_name": b.streetName,
                "station_code": b.id,
                "coordinates": (b.latitude, b.longitude),
                "slots": b.slots,
                "mechanical": b.mechanical_bikes,
                "electrical": b.electrical_bikes,
                "availability": b.disponibilidad,
                "distance_km": distance_km
            })

        for b in bus_stops:
            if not within_bbox(b.latitude, b.longitude):
                continue
            distance_km = DistanceHelper.haversine_distance(
                b.latitude, b.longitude, user_location.latitude, user_location.longitude
            ) if user_location else None
            if distance_km is not None and distance_km > max_distance_km:
                continue
            if not any(stop.get("station_code") == b.code and stop.get("type") == "bus" for stop in stops):
                stops.append({
                    "type": "bus",
                    "line_code": b.line_code,
                    "station_name": b.name,
                    "station_code": b.code,
                    "coordinates": (b.latitude, b.longitude),
                    "distance_km": distance_km
                })

        stops.sort(key=lambda x: (x["distance_km"] is None, x["distance_km"]))
        elapsed = time.perf_counter() - start
        logger.info(f"[DistanceHelper] build_stops_list ejecutado en {elapsed:.4f} s | {len(stops)} stops encontrados")
        return stops[:results_to_return]
    '''
