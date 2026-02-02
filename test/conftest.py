import os
import pytest
import asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from src.infrastructure.database.database import async_session_factory
from unittest.mock import MagicMock
from main import get_fastapi_app

# 1. Configuración para Asyncio
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

# 2. Cliente HTTP Asíncrono (Simula ser la App Android)
@pytest.fixture(scope="function")
async def client():
    app=get_fastapi_app()
    transport = ASGITransport(app=get_fastapi_app())
    headers = {
        "X-API-KEY": os.environ["BCN_TRANSIT_API_KEY"],
        "Content-Type": "application/json"
    }
    async with AsyncClient(transport=transport, base_url="http://localhost:8080", headers=headers) as c:
        yield c

# 3. Limpiador de Base de Datos (CRÍTICO)
@pytest.fixture(scope="function", autouse=True)
async def clean_db():
    """
    Se ejecuta AUTOMÁTICAMENTE antes de cada test.
    Borra los datos de usuarios y dispositivos para empezar limpio.
    """
    async with async_session_factory() as session:
        # Desactiva checks de FK temporalmente para truncar rápido
        await session.execute(text("TRUNCATE TABLE users CASCADE")) 
        await session.execute(text("TRUNCATE TABLE user_devices CASCADE"))
        # Agrega otras tablas si es necesario (favorites, logs...)
        await session.commit()

# 4. Mock de Firebase Auth
@pytest.fixture(scope="function")
def mock_firebase_auth(monkeypatch):
    """
    Engaña al backend para que crea que el token de Google es válido.
    """
    mock_verify = MagicMock()
    
    def side_effect(token):
        if token == "valid_google_token":
            return {
                "uid": "google_uid_123",
                "email": "test@gmail.com",
                "name": "Test User",
                "picture": "http://photo.url"
            }
        raise Exception("Invalid Token")

    monkeypatch.setattr("src.presentation.api.api.auth.verify_id_token", side_effect)
    return mock_verify