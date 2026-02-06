from datetime import datetime
from rapidfuzz import process, fuzz
from typing import Any, Callable, List, Optional
from src.application.utils.html_helper import HtmlHelper
from src.infrastructure.external.api.bicing_api_service import BicingApiService
from src.domain.schemas.models import DBBicingStation
from src.domain.models.common.nearby_station import NearbyStation
from src.infrastructure.database.repositories.bicing_repository import BicingRepository
from src.infrastructure.database.database import async_session_factory
from src.domain.models.bicing.bicing_station import BicingStation
from src.core.logger import logger

class BicingService:

    def __init__(self, bicing_api_service: BicingApiService):
        self.bicing_repository = BicingRepository(async_session_factory)
        self.bicing_api_service = bicing_api_service
        logger.info(f"[{self.__class__.__name__}] BicingService initialized (Standalone)")

    async def sync_stations(self):
        stations = await self.bicing_api_service.get_stations()
        
        stations_data = []
        for s in stations:
            if not s.latitude or not s.longitude:
                continue

            stations_data.append({
                "id": str(s.id),
                "name": s.streetName if s.streetName else "Unknown",                
                "latitude": self._safe_float(s.latitude),
                "longitude": self._safe_float(s.longitude),                
                "slots": self._safe_int(s.slots),
                "mechanical_bikes": self._safe_int(s.mechanical_bikes),
                "electrical_bikes": self._safe_int(s.electrical_bikes),                
                "availability": getattr(s, 'disponibilidad', 0),                
                "last_updated": datetime.utcnow()
            })
        
        if stations_data:
            await self.bicing_repository.upsert_all(stations_data)
        else:
            logger.warning("⚠️ No valid Bicing data to sync.")

    async def get_all_stations(self) -> List[BicingStation]:
        db_stations = await self.bicing_repository.get_all()
        return [self._map_db_to_domain(s) for s in db_stations]
        
    async def get_nearby_stations(self, lat: float, lon: float, radius: float = 0.5, limit=50) -> List[NearbyStation]:
        """
        Busca estaciones cercanas.
        Devuelve 'NearbyStation' para ser compatible con el Router, aunque no heredemos de ServiceBase.
        """
        # 1. Consulta SQL optimizada
        results = await self.bicing_repository.get_nearby(lat, lon, radius, limit)

        # 2. Mapeo a NearbyStation
        nearby_list = []
        for db_obj, distance in results:
            nearby_list.append(NearbyStation(
                type="bicing",
                station_name=db_obj.name,
                physical_station_id=str(db_obj.id),
                coordinates=(db_obj.latitude, db_obj.longitude),
                distance_km=distance,
                
                # Campos vacíos de transporte
                lines = [],
                
                # Campos específicos Bicing
                slots=db_obj.slots,
                mechanical=db_obj.mechanical_bikes,
                electrical=db_obj.electrical_bikes,
                availability= db_obj.availability
            ))
            
        return nearby_list

    async def get_stations_by_name(self, station_name: str) -> List[BicingStation]:
        """
        Búsqueda por nombre de calle.
        """
        stations = await self.get_all_stations()
        
        if not station_name:
            return stations
        
        return self.fuzzy_search(
                query=station_name, 
                items=stations, 
                key=lambda s: s.streetName, 
                threshold=75
            )

    async def get_station_by_id(self, station_id: str) -> Optional[BicingStation]:
        return await self.bicing_repository.get_by_id(station_id)

    # --- HELPERS ---

    def _map_db_to_domain(self, db_obj: DBBicingStation) -> BicingStation:
        return BicingStation(
            id=str(db_obj.id),
            type="bicing",
            streetName=db_obj.name,
            streetNumber=getattr(db_obj, 'street_number', "S/N"),
            latitude=db_obj.latitude,
            longitude=db_obj.longitude,
            slots=db_obj.slots,
            mechanical_bikes=db_obj.mechanical_bikes,
            electrical_bikes=db_obj.electrical_bikes,
            disponibilidad=db_obj.availability,
            bikes=db_obj.mechanical_bikes + db_obj.electrical_bikes,
            type_bicing="BIKE",
            status=getattr(db_obj, 'status', "OPN"),
            icon="",
            transition_start=None,
            transition_end=None,
            obcn=None
        )
    
    def _map_domain_to_db(self, obj: BicingStation) -> DBBicingStation:
        return DBBicingStation(
            id=str(obj.id),
            name=obj.streetName,
            latitude=obj.latitude,
            longitude=obj.longitude,
            slots=obj.slots,
            mechanical_bikes=obj.mechanical_bikes,
            electrical_bikes=obj.electrical_bikes,
            availability=obj.disponibilidad,         
            last_updated=datetime.utcnow() 
        )
    
    def _safe_float(self, value) -> float:
        """Convierte inputs sucios ('', None) a 0.0"""
        try:
            if value is None or value == "":
                return 0.0
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _safe_int(self, value) -> int:
        """Convierte inputs sucios ('', None) a 0"""
        try:
            if value is None or value == "":
                return 0
            return int(value)
        except (ValueError, TypeError):
            return 0

    def fuzzy_search(self, query: str, items: List[BicingStation], key: Callable[[Any], str], threshold: float = 80) -> List[Any]:
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