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

async def seed_lines(metro_service: MetroService, bus_service: BusService, tram_service: TramService,
                     rodalies_service: RodaliesService, fgc_service: FgcService):
    print("🚀 Iniciando Seeder de Líneas...")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    try:
        print("📥 Sincronizando todos los servicios...")
        
        await asyncio.gather(
            metro_service.sync_lines(),
            bus_service.sync_lines(),
            tram_service.sync_lines(),
            rodalies_service.sync_lines(),
            fgc_service.sync_lines(),
            return_exceptions=False
        )
        
        print("✨ Seeder completado con éxito.")

    except Exception as e:
        print(f"❌ Error crítico en el Seeder: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(seed_lines())