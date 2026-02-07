import asyncio
import sys
import os

from sqlalchemy import select

sys.path.append(os.getcwd())

from src.application.services.transport.bicing_service import BicingService
from src.domain.schemas.models import DBLine
from src.infrastructure.database.database import engine
from src.infrastructure.database.base import Base
from src.infrastructure.database.database import async_session_factory

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
    lines_map = {}

    try:        
        async with async_session_factory() as session:
            logger.info("🔍 Cargando mapa de líneas (Whitelist, Nombres & Colores)...")
            result = await session.execute(select(DBLine.id, DBLine.name, DBLine.color))
            
            lines_map = {
                row.id: {
                    "name": row.name,
                    "id": row.id,
                    "color": row.color or "333333"
                } 
                for row in result.all()
            }
            
            logger.info(f"✅ {len(lines_map)} líneas cargadas en memoria con metadatos.")

    except Exception as e:
        logger.error(f"❌ Error crítico obteniendo líneas: {e}")
        return

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    logger.info("📥 Sincronizando servicios en paralelo...")

    results = await asyncio.gather(
        metro_service.sync_stations(lines_map),
        bus_service.sync_stations(lines_map),
        rodalies_service.sync_stations(lines_map),
        tram_service.sync_stations(lines_map),
        fgc_service.sync_stations(lines_map),
        return_exceptions=True
    )

    for service_name, result in zip(["Metro", "Bus", "Rodalies", "Tram", "FGC"], results):
        if isinstance(result, Exception):
            logger.error(f"❌ {service_name} falló: {result}")
        else:
            logger.info(f"✅ {service_name} sincronizado.")

    logger.info("✨ Proceso de Stations Seeding finalizado.")

async def seed_alerts(metro_service: MetroService, bus_service: BusService, tram_service: TramService,
                     rodalies_service: RodaliesService, fgc_service: FgcService):
    logger.info("🚀 Iniciando Seeder de Alertas...")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    logger.info("📥 Sincronizando servicios en paralelo...")

    results = await asyncio.gather(
        metro_service.sync_alerts(),
        bus_service.sync_alerts(),
        rodalies_service.sync_alerts(),
        tram_service.sync_alerts(),
        fgc_service.sync_alerts(),
        return_exceptions=True
    )
    
    for service_name, result in zip(["Metro", "Bus", "Rodalies", "Tram", "FGC"], results):
        if isinstance(result, Exception):
            logger.error(f"❌ {service_name} falló: {result}")
        else:
            logger.info(f"✅ {service_name} sincronizado.")

    logger.info("✨ Proceso de Alerts Seeding finalizado.")
    
async def seed_bicing(bicing_service: BicingService):
    try:
        logger.info("📥 Sincronizando servicio de Bicing...")
        await bicing_service.sync_stations()
    except Exception as e:
        logger.error(f"❌ Error crítico en el Seeder: {e}")
        
    

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(seed_lines())