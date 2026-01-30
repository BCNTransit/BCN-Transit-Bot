from sqlalchemy import select, update, delete, and_
from sqlalchemy.orm import selectinload
from src.domain.schemas.models import DBUser, UserDevice, Favorite, DBUserSettings

class UserRepository:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    async def get_by_email(self, email: str) -> DBUser | None:
        """Busca usuario por email cargando sus dispositivos."""
        async with self.session_factory() as session:
            stmt = select(DBUser).options(selectinload(DBUser.devices)).where(DBUser.email == email)
            result = await session.execute(stmt)
            return result.scalars().first()

    async def get_user_by_installation_id(self, installation_id: str) -> DBUser | None:
        """
        Busca al dueño de un dispositivo específico.
        Fundamental para la MIGRACIÓN de anónimo a registrado.
        """
        async with self.session_factory() as session:
            stmt = (
                select(DBUser)
                .join(UserDevice)
                .where(UserDevice.installation_id == installation_id)
                .options(selectinload(DBUser.devices))
            )
            result = await session.execute(stmt)
            return result.scalars().first()

    async def merge_users(self, source_user_id: int, target_user_id: int):
        """
        Fusiona 'source_user' (anónimo) dentro de 'target_user' (google).
        1. Mueve dispositivos.
        2. Mueve favoritos (evitando duplicados).
        3. Mueve settings (si target no tiene).
        4. Elimina al usuario source.
        """
        async with self.session_factory() as session:
            try:
                # 1. MOVER DISPOSITIVOS
                # Todos los dispositivos del anónimo pasan a ser del usuario Google
                stmt_devices = update(UserDevice).where(
                    UserDevice.user_id == source_user_id
                ).values(user_id=target_user_id)
                await session.execute(stmt_devices)

                # 2. MOVER FAVORITOS (Gestionando duplicados)
                # Obtenemos favoritos de ambos para comparar
                stmt_source_favs = select(Favorite).where(Favorite.user_id == source_user_id)
                stmt_target_favs = select(Favorite).where(Favorite.user_id == target_user_id)
                
                res_source = await session.execute(stmt_source_favs)
                res_target = await session.execute(stmt_target_favs)
                
                source_favs = res_source.scalars().all()
                target_favs = res_target.scalars().all()

                # Creamos un set de claves únicas (Tipo + Código) de lo que YA tiene el destino
                existing_keys = {f"{f.transport_type}_{f.station_code}" for f in target_favs}

                for fav in source_favs:
                    key = f"{fav.transport_type}_{fav.station_code}"
                    
                    if key in existing_keys:
                        # CONFLICTO: Ya lo tiene -> Borramos el del anónimo
                        await session.delete(fav)
                    else:
                        # NO CONFLICTO: No lo tiene -> Se lo transferimos
                        fav.user_id = target_user_id
                        session.add(fav)

                # 3. MOVER SETTINGS
                # Verificamos si el usuario destino ya tiene configuración
                stmt_target_settings = select(DBUserSettings).where(DBUserSettings.user_id == target_user_id)
                res_settings = await session.execute(stmt_target_settings)
                target_settings = res_settings.scalars().first()

                if not target_settings:
                    # Si no tiene, movemos los del anónimo
                    stmt_move_settings = update(DBUserSettings).where(
                        DBUserSettings.user_id == source_user_id
                    ).values(user_id=target_user_id)
                    await session.execute(stmt_move_settings)
                else:
                    # Si ya tiene, borramos los del anónimo (sobran)
                    stmt_del_settings = delete(DBUserSettings).where(DBUserSettings.user_id == source_user_id)
                    await session.execute(stmt_del_settings)

                # 4. BORRAR USUARIO ANÓNIMO
                # Una vez vaciado de relaciones, lo eliminamos de la DB
                stmt_delete_user = delete(DBUser).where(DBUser.id == source_user_id)
                await session.execute(stmt_delete_user)

                await session.commit()
                
            except Exception as e:
                await session.rollback()
                raise e

    async def ensure_device_linked(self, user_id: int, installation_id: str, fcm_token: str):
        """
        Asegura que el dispositivo actual (installation_id) pertenezca al usuario indicado.
        Si existe y es de otro, lo mueve. Si no existe, lo crea. Actualiza token.
        """
        async with self.session_factory() as session:
            stmt = select(UserDevice).where(UserDevice.installation_id == installation_id)
            res = await session.execute(stmt)
            device = res.scalars().first()

            if device:
                # Si el dispositivo existe pero el user_id no coincide, lo corregimos
                if device.user_id != user_id:
                    device.user_id = user_id
                
                # Actualizamos el token FCM si ha cambiado
                if fcm_token and device.fcm_token != fcm_token:
                    device.fcm_token = fcm_token
                
                session.add(device)
            else:
                # Si no existe, lo creamos vinculado a este usuario
                new_dev = UserDevice(
                    user_id=user_id,
                    installation_id=installation_id,
                    fcm_token=fcm_token
                )
                session.add(new_dev)
            
            await session.commit()

    async def create_with_device(self, user: DBUser, device: UserDevice) -> DBUser:
        """
        Crea usuario y dispositivo en una sola transacción.
        TAMBIÉN crea los UserSettings por defecto.
        """
        async with self.session_factory() as session:
            session.add(user)
            user.devices.append(device)
            
            # Flush para obtener el ID del usuario recién creado
            await session.flush()
            
            # Crear settings por defecto para evitar nulos
            default_settings = DBUserSettings(user_id=user.id)
            session.add(default_settings)

            await session.commit()
            await session.refresh(user)
            return user

    async def add_device_to_user(self, user_id: int, device: UserDevice):
        """Añade un nuevo dispositivo a un usuario existente."""
        async with self.session_factory() as session:
            device.user_id = user_id
            session.add(device)
            await session.commit()

    async def update(self, user: DBUser):
        """Actualiza datos básicos del usuario."""
        async with self.session_factory() as session:
            await session.merge(user)
            await session.commit()