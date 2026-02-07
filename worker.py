import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from src.application.services.app_version_service import AppVersionService
from src.infrastructure.external.api.amb_api_service import AmbApiService, AmbGtfsStore
from src.core.logger import logger

# Servicios de Dominio
from src.application.services.transport.metro_service import MetroService
from src.application.services.transport.bus_service import BusService
from src.application.services.transport.tram_service import TramService
from src.application.services.transport.rodalies_service import RodaliesService
from src.application.services.transport.bicing_service import BicingService
from src.application.services.transport.fgc_service import FgcService

# Servicios Core
from src.application.services.cache_service import CacheService
from src.application.services.alerts_service import AlertsService
from src.application.services.secrets_manager import SecretsManager
from src.application.services.user_data_manager import UserDataManager

# APIs Externas
from src.infrastructure.external.api.tmb_api_service import TmbApiService
from src.infrastructure.external.api.tram_api_service import TramApiService
from src.infrastructure.external.api.rodalies_api_service import RodaliesApiService
from src.infrastructure.external.api.bicing_api_service import BicingApiService
from src.infrastructure.external.api.fgc_api_service import FgcApiService

# Base de Datos e Infraestructura
from src.infrastructure.localization.language_manager import LanguageManager
from src.infrastructure.database.database import init_db, reset_transport_data
from src.infrastructure.external.firebase_client import initialize_firebase as initialize_firebase_app
from src.infrastructure.database.seeders.seeder import seed_lines, seed_stations, seed_bicing, seed_alerts

class AppWorker:
    """
    Background worker centralizado.
    Gestiona la sincronización de datos, seeders y tareas programadas.
    """

    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        
        # Inicialización de Managers y APIs (se llenan en init_services)
        self.secrets_manager = SecretsManager()
        self.language_manager = LanguageManager()
        self.user_data_manager = UserDataManager()
        self.cache_service = CacheService()
        
        # APIs
        self.tmb_api_service = None
        self.tram_api_service = None
        self.rodalies_api_service = None
        self.bicing_api_service = None
        self.fgc_api_service = None

        # Servicios
        self.metro_service = None
        self.bus_service = None
        self.tram_service = None
        self.rodalies_service = None
        self.bicing_service = None
        self.fgc_service = None
        self.alerts_service = None

        self.app_version_service = None

    def init_services(self):
        logger.info("⚙️ Inicializando AppWorker Services...")
        
        try:
            # Carga de credenciales
            tmb_app_id = self.secrets_manager.get('TMB_APP_ID')
            tmb_app_key = self.secrets_manager.get('TMB_APP_KEY')
            tram_client_id = self.secrets_manager.get('TRAM_CLIENT_ID')
            tram_client_secret = self.secrets_manager.get('TRAM_CLIENT_SECRET')
        except Exception as e:
            logger.critical(f"❌ Error al cargar secretos: {e}")
            raise

        # APIs Externas
        self.tmb_api_service = TmbApiService(app_id=tmb_app_id, app_key=tmb_app_key)
        self.tram_api_service = TramApiService(client_id=tram_client_id, client_secret=tram_client_secret)
        self.rodalies_api_service = RodaliesApiService()
        self.bicing_api_service = BicingApiService()
        self.fgc_api_service = FgcApiService()

        # Servicios de transporte
        self.metro_service = MetroService(self.tmb_api_service, self.language_manager, self.cache_service, self.user_data_manager)
        self.bus_service = BusService(self.tmb_api_service, self.cache_service, self.user_data_manager, self.language_manager)
        self.tram_service = TramService(self.tram_api_service, self.language_manager, self.cache_service, self.user_data_manager)
        self.rodalies_service = RodaliesService(self.rodalies_api_service, self.language_manager, self.cache_service, self.user_data_manager)
        self.bicing_service = BicingService(self.bicing_api_service)
        self.fgc_service = FgcService(self.fgc_api_service, self.language_manager, self.cache_service, self.user_data_manager)

        # Alerts Service
        self.alerts_service = AlertsService(self.user_data_manager)

        # App Version Service
        self.app_version_service = AppVersionService()

        logger.info("✅ Todos los servicios inicializados correctamente.")

    # --- TAREAS DEL SCHEDULER ---

    async def task_sync_alerts(self):
        """Sincroniza alertas en tiempo real desde todas las APIs."""
        logger.info("🔔 [SCHEDULER] Sincronizando alertas de transporte...")
        try:
            await seed_alerts(
                self.metro_service, self.bus_service, 
                self.tram_service, self.rodalies_service, self.fgc_service
            )
        except Exception as e:
            logger.error(f"❌ Error en task_sync_alerts: {e}")

    async def task_daily_full_sync(self):
        """Ciclo diario: Actualización de GTFS y reconstrucción de tablas estáticas."""
        logger.info("🔄 [SCHEDULER] Iniciando ciclo diario de datos estáticos...")
        try:
            await asyncio.to_thread(AmbGtfsStore.load_data)
            await AmbGtfsStore.loading_event.wait()

            if not AmbGtfsStore.is_loaded:
                logger.error("❌ GTFS falló al cargar, abortando seeder.")
                return

            await reset_transport_data()
            await seed_lines(self.metro_service, self.bus_service, self.tram_service, self.rodalies_service, self.fgc_service)
            await seed_stations(self.metro_service, self.bus_service, self.tram_service, self.rodalies_service, self.fgc_service)
            await seed_bicing(self.bicing_service)
            
            logger.info("✅ [SCHEDULER] Ciclo diario de datos completado.")
        except Exception as e:
            logger.error(f"❌ Error en task_daily_full_sync: {e}")

    async def task_realtime_amb(self):
        """Actualiza el feed de posiciones y tiempos AMB (Cada 30s)."""
        try:
            await asyncio.to_thread(AmbGtfsStore.update_realtime_feed)
        except Exception as e:
            logger.error(f"❌ Error en RT AMB: {e}")

    async def task_bicing_sync(self):
        """Actualiza estado de estaciones Bicing (Cada 1m)."""
        try:
            await seed_bicing(self.bicing_service)
        except Exception as e:
            logger.error(f"❌ Error en RT Bicing: {e}")

    # --- CONTROL DE CICLO DE VIDA ---

    async def run(self):
        await init_db()
        initialize_firebase_app()
        await AmbApiService.initialize()

        # Configuración de Jobs en APScheduler
        
        # Alertas: Cada 5 minutos (Ajustable)
        self.scheduler.add_job(self.task_sync_alerts, trigger=IntervalTrigger(minutes=5), id='job_alerts')
        
        # Ciclo Completo: Cada madrugada a las 04:00
        self.scheduler.add_job(self.task_daily_full_sync, trigger=CronTrigger(hour=4, minute=0), id='job_daily')

        # Realtime AMB: Cada 30 segundos
        self.scheduler.add_job(self.task_realtime_amb, trigger=IntervalTrigger(seconds=30), id='job_rt_amb')

        # Bicing: Cada 60 segundos
        self.scheduler.add_job(self.task_bicing_sync, trigger=IntervalTrigger(seconds=60), id='job_bicing')

        # Ejecución inicial para asegurar datos frescos al arrancar el contenedor/proceso
        logger.info("🚀 AppWorker iniciado. Ejecutando sincronización inicial...")
        asyncio.create_task(self.task_sync_alerts())
        #asyncio.create_task(self.task_daily_full_sync())

        # Arrancar el scheduler y el servicio de notificaciones
        self.scheduler.start()
        await self.alerts_service.start()
        
        logger.info("✅ Scheduler operativo y tareas programadas.")

    async def shutdown(self):
        """Parada controlada del worker."""
        logger.info("🛑 Apagando AppWorker...")
        if self.scheduler.running:
            self.scheduler.shutdown()
        if self.alerts_service:
            await self.alerts_service.stop()