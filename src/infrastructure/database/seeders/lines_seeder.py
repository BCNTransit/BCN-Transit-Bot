import asyncio
import sys
import os

sys.path.append(os.getcwd())

from src.infrastructure.database.database import engine
from src.infrastructure.database.base import Base

from src.application.services.transport.metro_service import MetroService
from src.application.services.transport.bus_service import BusService
from src.application.services.transport.tram_service import TramService
from src.application.services.transport.rodalies_service import RodaliesService
from src.application.services.transport.fgc_service import FgcService

from src.core.logger import logger

async def seed_lines(metro_service: MetroService, bus_service: BusService, tram_service: TramService,
                     rodalies_service: RodaliesService, fgc_service: FgcService):
    logger.info("🚀 Iniciando Seeder de Líneas...")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    try:
        logger.info("📥 Sincronizando todos los servicios...")
        
        await asyncio.gather(
            metro_service.sync_lines(),
            bus_service.sync_lines(),
            tram_service.sync_lines(),
            rodalies_service.sync_lines(),
            fgc_service.sync_lines(),
            return_exceptions=False
        )
        
        logger.info("✨ Lines Seeder completado con éxito.")

    except Exception as e:
        logger.error(f"❌ Error crítico en el Seeder: {e}")

async def seed_stations(metro_service: MetroService, bus_service: BusService, tram_service: TramService,
                     rodalies_service: RodaliesService, fgc_service: FgcService):
    logger.info("🚀 Iniciando Seeder de Estaciones...")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    try:
        logger.info("📥 Sincronizando todos los servicios...")
        
        await asyncio.gather(
            metro_service.sync_stations(),
            bus_service.sync_stations(),
            rodalies_service.sync_stations(),
            tram_service.sync_stations(),
            fgc_service.sync_stations(),
            return_exceptions=False
        )
        
        logger.info("✨ Stations Seeder completado con éxito.")

    except Exception as e:
        logger.error(f"❌ Error crítico en el Seeder: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(seed_lines())