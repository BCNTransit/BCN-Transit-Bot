from sqlalchemy import NullPool, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from src.infrastructure.database.base import Base
from src.application.services.secrets_manager import SecretsManager

from src.core.logger import logger

secrets = SecretsManager()

DATABASE_URL = secrets.get("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError(
        "[Database] Error crítico: DATABASE_URL no encontrada en SecretsManager. "
    )

if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

engine = create_async_engine(
    DATABASE_URL,
    poolclass=NullPool,
    echo=False,
    pool_pre_ping=True
)

async_session_factory = async_sessionmaker(
    bind=engine,
    autoflush=False,
    expire_on_commit=False
)


async def get_db():
    async with async_session_factory() as session:
        try:
            yield session
        finally:
            await session.close()

async def init_db():
    async with engine.begin() as conn:
        import src.domain.schemas.models
        logger.info("🔄 Creando tablas en la base de datos...")
        await conn.run_sync(Base.metadata.create_all)
        logger.info("[Database] Tablas inicializadas correctamente.")

async def reset_transport_data():
    """
    Limpia TODAS las tablas de la nueva arquitectura de transporte.
    Borra: 
    1. Lines (Servicios)
    2. Physical Stations (Infraestructura)
    3. Route Stops (Relaciones/Rutas)
    """
    logger.info("🧹 Limpiando base de datos completa (Lines, Physical & Routes)...")
    
    async with async_session_factory() as session:
        try:
            await session.execute(text("""
                TRUNCATE TABLE 
                    physical_stations, 
                    lines, 
                    route_stops,
                    bicing_stations
                RESTART IDENTITY CASCADE;
            """))
            
            await session.commit()
            logger.info("✨ Base de datos impoluta. Tablas vacías y contadores a cero.")
            
        except Exception as e:
            logger.error(f"❌ Error limpiando tablas: {e}")
            await session.rollback()
            raise e