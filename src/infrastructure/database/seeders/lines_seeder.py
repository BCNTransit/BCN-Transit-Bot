import asyncio
import sys
import os

sys.path.append(os.getcwd())

from sqlalchemy.future import select
from src.infrastructure.database.database import async_session_factory, engine
from src.infrastructure.database.base import Base
from src.domain.schemas.models import LineModel
from src.domain.enums.transport_type import TransportType

from src.application.services.transport.metro_service import MetroService
from src.application.services.transport.bus_service import BusService
from src.application.services.transport.tram_service import TramService
from src.application.services.transport.rodalies_service import RodaliesService
from src.application.services.transport.fgc_service import FgcService

# --- LÓGICA DE COLORES ESTÁTICOS ---
RODALIES_COLORS = {
    "R1": "73B0DF", "R2": "009640", "R2 Nord": "AACB2B", "R2 Sud": "005F27",
    "R3": "E63027", "R4": "F6A22D", "R7": "BC79B2", "R8": "870064",
    "R11": "0064A7", "R13": "E8308A", "R14": "5E4295", "R15": "9A8B75",
    "R16": "B20933", "R17": "E87200", "RG1": "0071CE", "RT1": "00C4B3",
    "RT2": "E577CB", "RL3": "949300", "RL4": "FFDD00",
}

def resolve_color(name: str, transport_type: TransportType, api_color: str = None) -> str:
    if api_color and api_color not in ["", None, "null"]:
        return api_color.replace("#", "")

    if transport_type == TransportType.RODALIES:
        return RODALIES_COLORS.get(name, "808080")
    
    # Defaults
    if transport_type == TransportType.METRO: return "D9303D" # Rojo genérico TMB
    if transport_type == TransportType.BUS: return "D9303D"
    if transport_type == TransportType.FGC: return "F7931D"
    if transport_type == TransportType.TRAM: return "009640"
    
    return "808080"

async def seed_lines(metro_service: MetroService, bus_service: BusService, tram_service: TramService,
                     rodalies_service: RodaliesService, fgc_service: FgcService):
    print("🚀 Iniciando Seeder de Líneas...")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    all_raw_lines = []

    try:
        results = await asyncio.gather(
            metro_service.get_all_lines(),
            bus_service.get_all_lines(),
            tram_service.get_all_lines(),
            rodalies_service.get_all_lines(),
            fgc_service.get_all_lines(),
            return_exceptions=True
        )

        services_names = ["Metro", "Bus", "Tram", "Rodalies", "FGC"]

        for service_name, result in zip(services_names, results):
            if isinstance(result, Exception):
                print(f"⚠️ Error en {service_name}: {result}")
            else:
                all_raw_lines.extend(result)

    except Exception as e:
        print(f"❌ Error obteniendo datos de las APIs: {e}")
        return

    print(f"🔄 Procesando {len(all_raw_lines)} líneas para guardar en DB...")

    async with async_session_factory() as session:
        count = 0
        for raw in all_raw_lines:
            db_id = f"{raw.transport_type.value}-{raw.id}"            
            extra_data_dict = {}
            if raw.category:
                extra_data_dict["category"] = raw.category

            line_db = LineModel(
                id=db_id,                
                original_id=str(raw.id),                 
                code=str(raw.code),
                name=raw.name,
                description=raw.description,
                origin=raw.origin,
                destination=raw.destination,
                transport_type=raw.transport_type.value,
                color=resolve_color(raw.name, raw.transport_type, raw.color),
                extra_data=extra_data_dict or None
            )

            await session.merge(line_db)
            count += 1
        
        await session.commit()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(seed_lines())