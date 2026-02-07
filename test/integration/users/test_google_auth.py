import pytest
import uuid

from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from src.infrastructure.database.database_helper import DatabaseHelper
from src.domain.schemas.models import DBUser, DBUserCard, DBUserSettings, DBFavorite, UserDevice
from src.infrastructure.database.database import async_session_factory

ENDPOINT = "/api/users/auth/google"


# SCENARIO 1: MERGE
# Google user exists
# Anonymous user exists
# Anonymous user has data that google does not have -> merge data into Google user and remove anonymous
@pytest.mark.asyncio
async def test_google_login_scenario_1_merge(client, mock_firebase_auth):

    install_id_anonymous = str(uuid.uuid4())
    install_id_registered = str(uuid.uuid4())

    async with async_session_factory() as session:
        # 1. SETUP:
        # Creamos al Anónimo (Source)
        anon_user = await DatabaseHelper.insert_anonymous_user(session, install_id_anonymous)
        anon_id = anon_user.id
        
        # Le añadimos un Favorito al anónimo para probar que se mueve
        fav = DBFavorite(
            user_id=anon_id, 
            station_code="TEST_STATION", 
            transport_type="metro",
            station_name="Estación de Prueba"
        )
        session.add(fav)

        # Creamos al Google User (Target)
        google_user = await DatabaseHelper.insert_registered_user(session, install_id_registered)
        google_id = google_user.id
        
        await session.commit()
    
    # 2. ACCIÓN:
    # IMPORTANTE: Nos logueamos usando el dispositivo del ANÓNIMO.
    # Esto es lo que le dice al Backend que hay un conflicto de identidades.
    payload = {
        "id_token": "valid_google_token", # Mock: test@gmail.com
        "user_id": install_id_anonymous,  # <--- CLAVE DEL MERGE
        "fcm_token": "fcm_merge_new"
    }

    response = await client.post(ENDPOINT, json=payload)

    # 3. VERIFICACIÓN RESPUESTA
    assert response.status_code == 200
    data = response.json()
    
    assert data["status"] == "merged"
    assert data["message"] == "Accounts merged" # O el mensaje que hayas puesto
    assert int(data["user_id"]) == google_id # El ID superviviente es el de Google

    # 4. VERIFICACIÓN BASE DE DATOS
    async with async_session_factory() as session:
        # A. Verificar que el usuario Anónimo ha DESAPARECIDO
        anon_check = await session.get(DBUser, anon_id)
        assert anon_check is None, "El usuario anónimo debería haber sido borrado tras el merge"

        # B. Verificar que el usuario Google sigue vivo y tiene los datos
        stmt = (
            select(DBUser)
            .options(selectinload(DBUser.devices), selectinload(DBUser.favorites)) # Cargar favoritos
            .where(DBUser.id == google_id)
        )
        res = await session.execute(stmt)
        user = res.scalars().first()
        
        assert user is not None
        
        # C. Verificar Dispositivos (Debe tener 2: el suyo original + el del anónimo)
        device_ids = [d.installation_id for d in user.devices]
        assert install_id_anonymous in device_ids
        assert install_id_registered in device_ids
        assert len(user.devices) == 2
        
        # Verificar que el token del dispositivo fusionado se actualizó
        merged_device = next(d for d in user.devices if d.installation_id == install_id_anonymous)
        assert merged_device.fcm_token == "fcm_merge_new"

        # D. Verificar Favoritos (El favorito del anónimo ahora debe ser de Google)
        # Nota: Asumiendo que DBUser tiene relación 'favorites'
        assert len(user.favorites) >= 1
        fav_stations = [f.station_code for f in user.favorites]
        assert "TEST_STATION" in fav_stations



# SCENARIO 2: MERGE
# Google user exists
# Different Anonymous user exists
# Normal google login
@pytest.mark.asyncio
async def test_google_login_scenario_2(client, mock_firebase_auth):
    # ESCENARIO 2: El usuario vuelve a su propia cuenta
    
    install_id = str(uuid.uuid4())

    async with async_session_factory() as session:
        # 1. SETUP: Creamos usuario Google YA vinculado a este dispositivo
        # Usamos el helper pero "trucamos" el email para que sea el de Google
        user = await DatabaseHelper.insert_registered_user(session, install_id)
        user_id = user.id
        await session.commit()
    
    # 2. ACCIÓN: Login con el MISMO dispositivo
    payload = {
        "id_token": "valid_google_token",
        "user_id": install_id,
        "fcm_token": "token_actualizado"
    }

    response = await client.post(ENDPOINT, json=payload)
    
    # 3. CHECK: Debe ser "success", NO "merged"
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success" 
    
    # 4. Validar que se actualizó el token
    async with async_session_factory() as session:
        stmt = (
            select(DBUser)
            .options(selectinload(DBUser.devices), selectinload(DBUser.settings))
        )
        result = await session.execute(stmt)
        users = result.scalars().all()
        assert len(users) == 1, f"Se esperaba 1 usuario, hay {len(users)}"

        # Recuperamos el dispositivo directamente
        res = await session.execute(select(UserDevice).where(UserDevice.installation_id == install_id))
        dev = res.scalars().first()
        assert dev.fcm_token == "token_actualizado"


# SCENARIO 3: PROMOTION (Anonymous -> Google)
# Caso: El usuario es nuevo para Google (no hay email en DB).
#       Pero ya tiene una cuenta anónima en el dispositivo.
#       Resultado: La cuenta anónima se actualiza con los datos de Google.
@pytest.mark.asyncio
async def test_google_login_scenario_3_promotion(client, mock_firebase_auth):
    
    install_id = str(uuid.uuid4())
    fcm_token = "fcm_initial_token"

    # 1. SETUP: Creamos un usuario anónimo con datos previos
    async with async_session_factory() as session:
        anon_user = await DatabaseHelper.insert_anonymous_user(session, install_id)
        anon_id = anon_user.id

        fav = DBFavorite(
            user_id=anon_id, 
            station_code="TEST_STATION", 
            transport_type="metro",
            station_name="Estación de Prueba"
        )
        session.add(fav)

        await session.commit()

    # 2. ACCIÓN: El usuario hace Login con Google por primera vez
    # El mock de Firebase devolverá: email="test@gmail.com", name="Test User", etc.
    payload = {
        "id_token": "valid_google_token",
        "user_id": install_id,
        "fcm_token": "fcm_updated_123" # Actualizamos el token en el proceso
    }

    response = await client.post(ENDPOINT, json=payload)

    # 3. VERIFICACIÓN RESPUESTA
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "promoted"
    assert int(data["user_id"]) == anon_id # Sigue siendo el mismo ID de base de datos

    # 4. VERIFICACIÓN BASE DE DATOS
    async with async_session_factory() as session:
        # Verificación extra: No deben existir más usuarios en la tabla
        res_total = await session.execute(select(func.count()).select_from(DBUser))
        total_users = res_total.scalar()
        assert total_users == 1, f"Se esperaba 1 solo usuario en la DB, pero hay {total_users}"

        stmt = (
            select(DBUser)
            .options(
                selectinload(DBUser.devices),
                selectinload(DBUser.favorites),
                selectinload(DBUser.user_cards)
            )
            .where(DBUser.id == anon_id)
        )
        result = await session.execute(stmt)
        user = result.scalars().first()

        assert user is not None, "El usuario debería existir tras la promoción"

        # A. Verificar que los datos de Google se han aplicado
        assert user.email == "test@gmail.com"
        assert user.username == "Test User"
        assert user.firebase_uid is not None
        
        # B. Verificar que NO ha perdido sus datos antiguos
        assert len(user.favorites) == 1
        assert user.favorites[0].station_code == "TEST_STATION"
        
        assert len(user.user_cards) == 1
        assert user.user_cards[0].name == "Card Anonymous"

        # C. Verificar que el dispositivo sigue ahí y se actualizó el FCM
        assert len(user.devices) == 1
        assert user.devices[0].fcm_token == "fcm_updated_123"
        assert user.devices[0].installation_id == install_id

# SCENARIO 4:
# Google user exists
# Anonymous user DOES NOT exist
@pytest.mark.asyncio
async def test_google_login_scenario_4(client, mock_firebase_auth):
    """Escenario 4: Registro limpio con Google"""

    user_id = str(uuid.uuid4())
    
    payload = {
        "id_token": "valid_google_token",
        "user_id": user_id,
        "fcm_token": "fcm_123"
    }
    
    response = await client.post(ENDPOINT, json=payload)
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "created"
    assert "user_id" in data

    async with async_session_factory() as session:
        stmt = (
            select(DBUser)
            .options(selectinload(DBUser.devices), selectinload(DBUser.settings))
        )
        result = await session.execute(stmt)
        users = result.scalars().all()
        
        assert len(users) == 1, f"Se esperaba 1 usuario, hay {len(users)}"
        user = users[0]

        assert user.username == "Test User"
        assert user.email == "test@gmail.com"

        assert len(user.devices) == 1, f"El usuario debería tener 1 dispositivo, tiene {len(user.devices)}"
        device = user.devices[0]
        assert device.fcm_token == "fcm_123"
        assert device.installation_id == user_id

        assert user.settings is not None, "No se crearon los settings para el nuevo usuario"
        
        res_devices_count = await session.execute(select(func.count()).select_from(UserDevice))
        assert res_devices_count.scalar() == 1

        res_settings_count = await session.execute(select(func.count()).select_from(DBUserSettings))
        assert res_settings_count.scalar() == 1