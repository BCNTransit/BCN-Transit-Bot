from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base

from src.application.services.secrets_manager import SecretsManager

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
    echo=False,
    pool_pre_ping=True
)

async_session_factory = async_sessionmaker(
    bind=engine,
    autoflush=False,
    expire_on_commit=False
)

Base = declarative_base()

async def get_db():
    async with async_session_factory() as session:
        try:
            yield session
        finally:
            await session.close()

async def init_db():
    async with engine.begin() as conn:
        import src.domain.schemas.models as models
                
        await conn.run_sync(Base.metadata.create_all)
        print("[Database] Tablas inicializadas correctamente.")

async def reset_transport_data():
    """
    Limpia las tablas de líneas y estaciones para asegurar una carga limpia.
    Usa TRUNCATE CASCADE para borrar datos y reiniciar IDs.
    """
    print("🧹 Limpiando base de datos (Lines & Stations)...")
    
    async with async_session_factory() as session:
        try:
            await session.execute(text("TRUNCATE TABLE stations, lines RESTART IDENTITY CASCADE;"))
            
            await session.commit()
            print("✨ Tablas limpias.")
        except Exception as e:
            print(f"❌ Error limpiando tablas: {e}")
            await session.rollback()