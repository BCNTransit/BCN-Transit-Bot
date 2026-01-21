# src/infrastructure/repositories/user_repository.py

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from src.domain.schemas.models import DBUser, UserDevice

class UserRepository:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    async def get_by_email(self, email: str) -> DBUser | None:
        """Busca usuario por email cargando sus dispositivos."""
        async with self.session_factory() as session:
            # Usamos selectinload para traer los dispositivos asociados (útil para verificar si ya tiene este móvil)
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

    async def create_with_device(self, user: DBUser, device: UserDevice) -> DBUser:
        """Crea usuario y dispositivo en una sola transacción atómica."""
        async with self.session_factory() as session:
            session.add(user)
            user.devices.append(device)
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
        """Actualiza datos del usuario."""
        async with self.session_factory() as session:
            await session.merge(user)
            await session.commit()