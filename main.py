import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
import uvicorn

from src.infrastructure.external.api.amb_api_service import AmbApiService
from src.core.logger import logger

from src.presentation.api.server import create_app
from src.infrastructure.database.database import init_db
from src.infrastructure.external.firebase_client import initialize_firebase as initialize_firebase_app
from worker import AppWorker


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gestiona el ciclo de vida de FastAPI.
    """
    logger.info("🚀 API Lifespan: Inicializando recursos...")
    await AmbApiService.initialize()    
    yield    
    logger.info("🛑 API Lifespan: Cerrando recursos...")

async def start_fastapi(app: FastAPI):
    """
    Configuración y arranque del servidor Uvicorn.
    """
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=8000,
        loop="uvloop",
        log_level="info",
        timeout_keep_alive=5
    )
    server = uvicorn.Server(config)
    await server.serve()

async def main():
    """
    Punto de entrada único.
    Arranque paralelo de Tareas en Segundo Plano (Worker) y API.
    """
    worker = AppWorker()
    worker.init_services()

    app = create_app(
        metro_service=worker.metro_service,
        bus_service=worker.bus_service,
        tram_service=worker.tram_service,
        rodalies_service=worker.rodalies_service,
        bicing_service=worker.bicing_service,
        fgc_service=worker.fgc_service,
        user_data_manager=worker.user_data_manager,
        lifespan=lifespan
    )

    logger.info("📡 Iniciando sistema dual: AppWorker + FastAPI")

    try:
        await asyncio.gather(
            worker.run(),
            start_fastapi(app)
        )
    except KeyboardInterrupt:
        logger.info("👋 Detención manual detectada.")
    except Exception as e:
        logger.error(f"❌ Error crítico en el hilo principal: {e}")
    finally:
        await worker.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass