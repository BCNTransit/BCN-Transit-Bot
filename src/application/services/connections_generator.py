import unicodedata
import re
from collections import defaultdict
from typing import List

# Modelos DB (SQLAlchemy)

# Modelos Dominio (Pydantic)
from src.domain.schemas.models import DBStation
from src.domain.models.common.connections import Connections
from src.domain.models.common.line import Line

from src.infrastructure.database.repositories.stations_repository import StationsRepository
from src.infrastructure.database.database import async_session_factory
from src.core.logger import logger

class ConnectionsGenerator:
    def __init__(self):
        self.repo = StationsRepository(async_session_factory)

    async def generate_and_save_connections(self):
        logger.info("🔗 [ConnectionsGenerator] Iniciando cálculo de transbordos...")
        
        all_stations = await self.repo.get_all_raw()
        
        if not all_stations:
            logger.warning("⚠️ No hay estaciones en la DB para conectar.")
            return

        groups = defaultdict(list)
        for st in all_stations:
            key = self._get_grouping_key(st)
            if key:
                groups[key].append(st)

        stations_to_update = []

        for group_key, stations_in_group in groups.items():
            if len(stations_in_group) < 2:
                continue

            for current_station in stations_in_group:
                lines_list: List[Line] = []
                
                for other_station in stations_in_group:
                    if current_station.id == other_station.id:
                        continue
                    
                    if current_station.line_id == other_station.line_id:
                        continue

                    line_sql = other_station.line
                    if line_sql:
                        line_dto = Line(
                            id=line_sql.id,               # ej: "metro-L1"
                            original_id=line_sql.original_id, # ej: "L1"
                            code=line_sql.code,           # ej: "L1"
                            name=line_sql.name,           # ej: "L1"
                            color=line_sql.color,         # ej: "FF0000"
                            transport_type=line_sql.transport_type,
                            
                            stations=[], 
                            
                            description=line_sql.description,
                            origin=line_sql.origin,
                            destination=line_sql.destination,
                            has_alerts=False,
                            extra_data=None
                        )
                        lines_list.append(line_dto)

                if lines_list:
                    connections_obj = Connections(lines=lines_list)
                    
                    current_station.connections_data = connections_obj.model_dump(mode='json')
                    stations_to_update.append(current_station)

        if stations_to_update:
            batch_size = 500
            total = len(stations_to_update)
            logger.info(f"💾 Guardando conexiones para {total} estaciones...")
            
            for i in range(0, total, batch_size):
                batch = stations_to_update[i : i + batch_size]
                await self.repo.upsert_many(batch)
                
            logger.info(f"✅ Conexiones generadas correctamente.")
        else:
            logger.info("ℹ️ No se encontraron nuevas conexiones.")

    def _get_grouping_key(self, station: DBStation) -> str:
        """Determina la clave de agrupación (Group Code > Moute ID > Nombre Normalizado)."""
        if station.extra_data:
            g_code = station.extra_data.get("station_group_code") or \
                     station.extra_data.get("CODI_GRUP_ESTACIO") or \
                     station.extra_data.get("moute_id")
            if g_code:
                return f"GROUP_{g_code}"

        return self._normalize_name(station.name)

    def _normalize_name(self, name: str) -> str:
        """Limpia el nombre para maximizar coincidencias (fuzzy match manual)."""
        if not name: return ""
        n = name.lower()
        n = ''.join(c for c in unicodedata.normalize('NFD', n) if unicodedata.category(c) != 'Mn')
        
        stopwords = ["estacio", "station", "parada", "pl.", "plaça", "plaza", 
                     "av.", "avinguda", "avenida", "c/", "carrer", "calle", 
                     "pg.", "passeig", "rambla", "de", "del", "els", "les", "la", "el"]
        
        for word in stopwords:
            n = re.sub(rf'\b{re.escape(word)}\b', '', n)
            
        n = re.sub(r'[^a-z0-9]', '', n)
        return n