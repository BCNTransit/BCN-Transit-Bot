import pytest
import uuid
from sqlalchemy import func, select
from src.infrastructure.database.database_helper import DatabaseHelper
from src.domain.schemas.models import DBUser, UserDevice
from src.infrastructure.database.database import async_session_factory
from sqlalchemy.orm import selectinload

ENDPOINT_LOGOUT = "/api/users/auth/logout"

@pytest.mark.asyncio
async def test_logout_successful(client, mock_firebase_auth):
    install_id = str(uuid.uuid4())
    
    async with async_session_factory() as session:
        user = await DatabaseHelper.insert_registered_user(session, install_id)
        user_id = user.id
        await session.commit()

    headers = {"Authorization": "Bearer valid_google_token"}
    payload = {
        "installation_id": install_id
    }

    response = await client.post(ENDPOINT_LOGOUT, json=payload, headers=headers)

    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Logged out successfully. Device unlinked."

    async with async_session_factory() as session:
        stmt_device = select(UserDevice).where(UserDevice.installation_id == install_id)
        res_device = await session.execute(stmt_device)
        device = res_device.scalars().first()
        assert device is None, "El dispositivo específico debería haber sido borrado de la DB"

        stmt_user = (
            select(DBUser)
            .options(selectinload(DBUser.devices))
            .where(DBUser.id == user_id)
        )
        res_user = await session.execute(stmt_user)
        user_db = res_user.scalars().first()
        
        assert user_db is not None, "El usuario de Google debe persistir tras el logout"
        assert user_db.email == "test@gmail.com"
        
        assert len(user_db.devices) == 0, f"El usuario no debería tener dispositivos, tiene {len(user_db.devices)}"

        res_count = await session.execute(select(func.count()).select_from(UserDevice))
        total_devices = res_count.scalar()
        assert total_devices == 0, f"La tabla de dispositivos debería estar vacía, hay {total_devices}"

@pytest.mark.asyncio
async def test_logout_wrong_device_or_user(client, mock_firebase_auth):
    install_id_real = str(uuid.uuid4())
    install_id_fake = "wrong_id_123"

    async with async_session_factory() as session:
        await DatabaseHelper.insert_registered_user(session, install_id_real)
        await session.commit()

    headers = {"Authorization": "Bearer valid_google_token"}
    payload = {"installation_id": install_id_fake}

    response = await client.post(ENDPOINT_LOGOUT, json=payload, headers=headers)

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()