from sqlalchemy import func, select, update, delete, and_
from sqlalchemy.orm import selectinload
from sqlalchemy import update as sql_update
from src.domain.schemas.models import DBUser, DBUserCard, UserDevice, DBFavorite, DBUserSettings

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
        
    async def register_device_entry(self, user_id: int, installation_id: str, fcm_token: str):
        async with self.session_factory() as session:
            if fcm_token:
                await self._clean_duplicate_tokens(session, fcm_token, installation_id)

            stmt = select(UserDevice).where(UserDevice.installation_id == str(installation_id))
            res = await session.execute(stmt)
            device = res.scalars().first()

            if device:
                device.user_id = user_id
                if fcm_token:
                    device.fcm_token = fcm_token
            else:
                device = UserDevice(
                    user_id=user_id, 
                    installation_id=str(installation_id), 
                    fcm_token=fcm_token
                )
                session.add(device)

            await session.commit()    
        
    async def remove_device(self, user_id: int, installation_id: str) -> bool:
        async with self.session_factory() as session:
            stmt = select(UserDevice).where(
                UserDevice.installation_id == installation_id,
                UserDevice.user_id == user_id
            )
            result = await session.execute(stmt)
            device = result.scalars().first()

            if device:
                await session.delete(device)
                await session.commit()
                return True
            return False

    async def merge_users(self, source_user_id: int, target_user_id: int):
        async with self.session_factory() as session:
            try:
                # 1. MOVER DISPOSITIVOS (Update directo)
                stmt_devices = update(UserDevice).where(
                    UserDevice.user_id == source_user_id
                ).values(user_id=target_user_id)
                await session.execute(stmt_devices)

                # 2. MOVER FAVORITOS (Lógica ORM)
                stmt_source_favs = select(DBFavorite).where(DBFavorite.user_id == source_user_id)
                stmt_target_favs = select(DBFavorite).where(DBFavorite.user_id == target_user_id)
                
                res_source = await session.execute(stmt_source_favs)
                res_target = await session.execute(stmt_target_favs)
                
                source_favs = res_source.scalars().all()
                target_favs = res_target.scalars().all()

                existing_fav_keys = {f"{f.transport_type}_{f.station_code}" for f in target_favs}

                for fav in source_favs:
                    key = f"{fav.transport_type}_{fav.station_code}"
                    if key in existing_fav_keys:
                        await session.delete(fav)
                    else:
                        fav.user_id = target_user_id
                        session.add(fav)

                # 3. MOVER TARJETAS (User Cards) - NUEVO BLOQUE
                # ---------------------------------------------------------
                stmt_source_cards = select(DBUserCard).where(DBUserCard.user_id == source_user_id)
                stmt_target_cards = select(DBUserCard).where(DBUserCard.user_id == target_user_id)

                res_source_cards = await session.execute(stmt_source_cards)
                res_target_cards = await session.execute(stmt_target_cards)

                source_cards = res_source_cards.scalars().all()
                target_cards = res_target_cards.scalars().all()

                # Identificamos las tarjetas que el destino YA tiene por su Nombre
                # (Si prefieres permitir nombres duplicados, quita esta lógica y mueve todo directametne)
                existing_card_names = {c.name for c in target_cards}

                for card in source_cards:
                    if card.name in existing_card_names:
                        # CONFLICTO: El usuario ya tiene una tarjeta con este nombre.
                        # Borramos la del anónimo para no tener dos "T-Jove" duplicadas.
                        await session.delete(card)
                    else:
                        # NO CONFLICTO: Se la transferimos
                        card.user_id = target_user_id
                        session.add(card)
                # ---------------------------------------------------------

                # 4. MOVER SETTINGS (Lógica ORM/SQL mixta)
                stmt_target_settings = select(DBUserSettings).where(DBUserSettings.user_id == target_user_id)
                res_settings = await session.execute(stmt_target_settings)
                target_settings = res_settings.scalars().first()

                if not target_settings:
                    # Si target no tiene settings, movemos los del source
                    stmt_move_settings = update(DBUserSettings).where(
                        DBUserSettings.user_id == source_user_id
                    ).values(user_id=target_user_id)
                    await session.execute(stmt_move_settings)
                else:
                    # Si target ya tiene, borramos los del source (sobran)
                    stmt_del_settings = delete(DBUserSettings).where(DBUserSettings.user_id == source_user_id)
                    await session.execute(stmt_del_settings)

                # CRÍTICO: Flush para impactar cambios ORM (Favoritos y Cards) antes de borrar User
                await session.flush()

                # 5. BORRAR USUARIO ANÓNIMO
                stmt_delete_user = delete(DBUser).where(DBUser.id == source_user_id)
                await session.execute(stmt_delete_user)

                await session.commit()

            except Exception as e:
                await session.rollback()
                raise e

    async def ensure_device_linked(self, user_id: int, installation_id: str, fcm_token: str):
        """
        Asegura que el dispositivo (installation_id) pertenezca al usuario indicado.
        Si era de otro usuario (anónimo), se lo quita y borra al anónimo si queda huérfano.
        """
        async with self.session_factory() as session:
            # 1. Buscar si el dispositivo ya existe
            stmt = select(UserDevice).where(UserDevice.installation_id == installation_id)
            res = await session.execute(stmt)
            device = res.scalars().first()     

            if device:
                # --- CASO A: El dispositivo ya existe ---
                # Verificar si pertenece a OTRA persona
                if device.user_id != user_id:
                    old_user_id = device.user_id  # Guardamos quién era el dueño
                    device.user_id = user_id      # Cambiamos de dueño (Robo)
                    session.add(device)

                    # Forzamos que la DB sepa que el dispositivo ya cambió de manos
                    await session.flush()

                    # --- LOGIC GHOST BUSTER ---
                    # Revisamos si el dueño anterior ha quedado como un usuario fantasma
                    old_user = await session.get(DBUser, old_user_id)

                    # Solo nos preocupa si el usuario anterior era ANÓNIMO (sin email)
                    if old_user and old_user.email is None:
                        # Contamos cuántos dispositivos le quedan
                        stmt_count = select(func.count()).select_from(UserDevice).where(UserDevice.user_id == old_user_id)
                        res_count = await session.execute(stmt_count)
                        remaining_devices = res_count.scalar()

                        # Si se quedó con 0 dispositivos, lo borramos del sistema
                        if remaining_devices == 0:
                            await session.delete(old_user)

                # Actualizar Token FCM si es necesario
                if fcm_token and device.fcm_token != fcm_token:
                    device.fcm_token = fcm_token
                    session.add(device)

            else:
                # --- CASO B: El dispositivo es totalmente nuevo ---
                new_dev = UserDevice(
                    user_id=user_id,
                    installation_id=installation_id,
                    fcm_token=fcm_token
                )
                session.add(new_dev)

            await session.commit()

    async def create_with_device(self, user: DBUser, device: UserDevice) -> DBUser:
        """
        Crea usuario + dispositivo + settings.
        Elimina dispositivos viejos que usen el mismo token.
        """
        async with self.session_factory() as session:
            if device.fcm_token:
                await self._clean_duplicate_tokens(
                    session, 
                    device.fcm_token, 
                    device.installation_id
                )

            session.add(user)
            user.devices.append(device)
            
            await session.flush()
            
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
        """
        Actualiza SOLO los campos del usuario, sin tocar sus relaciones (dispositivos).
        """
        async with self.session_factory() as session:
            stmt = (
                sql_update(DBUser)
                .where(DBUser.id == user.id)
                .values(
                    username=user.username,
                    photo_url=user.photo_url,
                    firebase_uid=user.firebase_uid,
                    email=user.email,
                    source=user.source,
                )
                .execution_options(synchronize_session="fetch")
            )
            
            await session.execute(stmt)
            await session.commit()

    async def _clean_duplicate_tokens(self, session, fcm_token: str, current_installation_id: str):
        if not fcm_token:
            return

        stmt = delete(UserDevice).where(
            and_(
                UserDevice.fcm_token == fcm_token,
                UserDevice.installation_id != str(current_installation_id)
            )
        )
        await session.execute(stmt)