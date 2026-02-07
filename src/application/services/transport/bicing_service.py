from datetime import datetime
from rapidfuzz import process, fuzz
from typing import Any, Callable, List, Optional
from src.domain.models.common.search_result import StationSearchResult
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

    async def get_stations_by_name(self, station_name: str) -> List[StationSearchResult]:
        db_stations = await self.bicing_repository.get_all()
        
        if not station_name:
            return db_stations
        
        db_stations = await self.bicing_repository.get_all()
        
        stations = [
            self._map_db_bicing_to_station_search_result(db_station) 
            for db_station in db_stations
        ]
        
        return self.fuzzy_search(
                query=station_name, 
                items=stations, 
                key=lambda s: s.station_name, 
                threshold=75
            )

    async def get_station_by_id(self, station_id: str) -> Optional[BicingStation]:
        id = station_id.split("-")[1] if "-" in station_id else station_id
        station = await self.bicing_repository.get_by_id(id)
        return self._map_db_to_domain(station)

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
            type_bicing=1,
            status=getattr(db_obj, 'status', 1),
            icon="",
            transition_start=None,
            transition_end=None,
            obcn=None
        )
    
    def _map_db_bicing_to_station_search_result(self, db_obj: DBBicingStation) -> StationSearchResult:
        total_bikes = (db_obj.mechanical_bikes or 0) + (db_obj.electrical_bikes or 0)

        return StationSearchResult(
            physical_station_id=f"bicing-{str(db_obj.id)}",
            station_external_code=str(db_obj.id),
            line_id="bicing",
            
            station_name=db_obj.name,
            line_name="Bicing",
            line_color="#FF0000",
            line_destination=f"Bicis: {total_bikes} | Huecos: {db_obj.slots}",
            
            type="BICING",
            match_score=0.0,
            
            coordinates=(db_obj.latitude, db_obj.longitude),
            has_alerts=(total_bikes == 0 and db_obj.slots == 0)
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

    def fuzzy_search(self, query: str, items: List[StationSearchResult], key: Callable[[Any], str], threshold: float = 75) -> List[StationSearchResult]:
        query_lower = query.lower()
        query_norm = HtmlHelper.normalize_text(query_lower)

        # 1. Exact Matches (Puntuación máxima)
        exact_matches = []
        for item in items:
            if query_lower in key(item).lower():
                item.match_score = 100.0
                exact_matches.append(item)

        # 2. Normalized Matches (Puntuación alta, pero menor que exacta)
        # Excluimos lo que ya encontramos en exact_matches
        remaining_after_exact = [item for item in items if item not in exact_matches]
        normalized_matches = []
        for item in remaining_after_exact:
            if query_norm in HtmlHelper.normalize_text(key(item).lower()):
                item.match_score = 95.0
                normalized_matches.append(item)

        # 3. Fuzzy Matches (Puntuación real de la librería)
        # Solo procesamos lo que no ha coincidido con los métodos anteriores
        all_found = exact_matches + normalized_matches
        remaining_items = [item for item in items if item not in all_found]
        
        # Creamos un mapa para recuperar el objeto por su nombre/key
        item_dict = {key(item): item for item in remaining_items}
        
        fuzzy_results = process.extract(
            query=query,
            choices=item_dict.keys(),
            scorer=fuzz.WRatio,
            limit=20 # Limitamos para no procesar miles de resultados irrelevantes
        )

        fuzzy_filtered = []
        for name, score, _ in fuzzy_results:
            if score >= threshold:
                item = item_dict[name]
                item.match_score = float(score)
                fuzzy_filtered.append(item)

        # Devolvemos todo combinado
        return exact_matches + normalized_matches + fuzzy_filtered