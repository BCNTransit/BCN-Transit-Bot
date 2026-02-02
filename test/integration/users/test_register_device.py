import pytest
import uuid

from sqlalchemy import func, select

from src.domain.schemas.models import DBUser, DBUserSettings, UserDevice
from src.infrastructure.database.database import async_session_factory
from src.infrastructure.database.database_helper import DatabaseHelper

ENDPOINT = "/api/users/register-device"

@pytest.mark.asyncio
async def test_register_new_device(client):
    install_id = str(uuid.uuid4())
    
    payload = {
        "installation_id": install_id,
        "fcm_token": "token_A"
    }
    
    response = await client.post(ENDPOINT, json=payload)
    
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "ok"
    assert data["is_new_user"] is True

    async with async_session_factory() as session:
        res_device = await session.execute(
            select(func.count()).select_from(UserDevice).where(UserDevice.installation_id == install_id)
        )
        count_devices = res_device.scalar()

        res_settings = await session.execute(
            select(func.count()).select_from(DBUserSettings)
        )
        count_settings = res_settings.scalar()
        
        res_user = await session.execute(
            select(func.count()).select_from(DBUser)
        )
        count_users = res_user.scalar()

        assert count_devices == 1, f"Se esperaba 1 dispositivo, se encontraron {count_devices}"
        assert count_users == 1, f"Se esperaba 1 usuario en la DB, se encontraron {count_users}"
        assert count_settings == 1, f"Se esperaba 1 user settings, se encontraron {count_settings}"

@pytest.mark.asyncio
async def test_register_already_existing(client):

    install_id = str(uuid.uuid4())
    async with async_session_factory() as session:
        await DatabaseHelper.insert_anonymous_user(session, install_id)
        await session.commit()
    
    payload = {
        "installation_id": install_id,
        "fcm_token": "token_A"
    }
    
    response = await client.post(ENDPOINT, json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "ok" and data["is_new_user"] == False

    async with async_session_factory() as session:
        res_device = await session.execute(
            select(func.count()).select_from(UserDevice).where(UserDevice.installation_id == install_id)
        )
        count_devices = res_device.scalar()

        res_settings = await session.execute(
            select(func.count()).select_from(DBUserSettings)
        )
        count_settings = res_settings.scalar()
        
        res_user = await session.execute(
            select(func.count()).select_from(DBUser)
        )
        count_users = res_user.scalar()

        assert count_devices == 1, f"Se esperaba 1 dispositivo, se encontraron {count_devices}"
        assert count_users == 1, f"Se esperaba 1 usuario en la DB, se encontraron {count_users}"
        assert count_settings == 1, f"Se esperaba 1 user settings, se encontraron {count_settings}"

@pytest.mark.asyncio
async def test_register_no_fcm_token(client):
    install_id = str(uuid.uuid4())
    
    payload = {
        "installation_id": install_id,
        "fcm_token": ""
    }
    
    response = await client.post(ENDPOINT, json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "ok" and data["is_new_user"] == True

    async with async_session_factory() as session:
        res_device = await session.execute(
            select(func.count()).select_from(UserDevice).where(UserDevice.installation_id == install_id)
        )
        count_devices = res_device.scalar()

        res_settings = await session.execute(
            select(func.count()).select_from(DBUserSettings)
        )
        count_settings = res_settings.scalar()
        
        res_user = await session.execute(
            select(func.count()).select_from(DBUser)
        )
        count_users = res_user.scalar()

        assert count_devices == 1, f"Se esperaba 1 dispositivo, se encontraron {count_devices}"
        assert count_users == 1, f"Se esperaba 1 usuario en la DB, se encontraron {count_users}"
        assert count_settings == 1, f"Se esperaba 1 user settings, se encontraron {count_settings}"