import asyncio
import inspect
import logging
from typing import List, Optional
from datetime import datetime, timezone
from functools import wraps

# SQLAlchemy & DB
from fastapi import HTTPException
from sqlalchemy import select, delete, update, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from src.domain.models.common.card import CardCreate, CardResponse
from src.infrastructure.database.database import async_session_factory
from src.domain.enums.clients import ClientType
from src.domain.enums.transport_type import TransportType

# Domain Models (El nuevo Pydantic Model)
from src.domain.models.common.user import User 
from src.domain.models.common.alert import Alert, AffectedEntity, Publication

# Database Models
from src.domain.schemas.models import (
    DBUser,
    DBUserCard, 
    UserDevice as DBUserDevice,
    Favorite as DBFavorite, 
    Alert as DBAlert, 
    AuditLog as DBAuditLog, 
    DBSearchHistory,
    UserSource,
    DBNotificationLog
)

# Domain Schemas
from src.domain.schemas.favorite import FavoriteResponse

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# DECORADOR DE AUDITORÍA (Sin cambios, se mantiene tu versión corregida)
# -------------------------------------------------------------------------
def audit_action(action_type: str, params_args: list = None):
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            try:
                result = await func(self, *args, **kwargs)
                status = "SUCCESS"
                error_info = None
            except Exception as e:
                result = e
                status = "ERROR"
                error_info = str(e)

            try:
                sig = inspect.signature(func)
                bound_args = sig.bind(self, *args, **kwargs)
                bound_args.apply_defaults()
                func_args = bound_args.arguments

                raw_source = func_args.get("client_source", "UNKNOWN")
                if hasattr(raw_source, "value"):
                    source_str = str(raw_source.value)
                elif hasattr(raw_source, "name"):
                    source_str = str(raw_source.name)
                else:
                    source_str = str(raw_source)

                raw_user_id = func_args.get("user_id")
                user_id_ext = str(raw_user_id) if raw_user_id is not None else None

                details = {"params": {}, "status": status}
                if error_info: details["error"] = error_info

                if params_args:
                    for param_name in params_args:
                        val = func_args.get(param_name)
                        if val is not None:
                            if hasattr(val, "value"): 
                                details["params"][param_name] = str(val.value)
                            elif hasattr(val, "model_dump_json"): 
                                details["params"][param_name] = val.model_dump_json()
                            elif hasattr(val, "dict"): 
                                details["params"][param_name] = str(val.dict())
                            else: 
                                details["params"][param_name] = str(val)
                        else:
                            details["params"][param_name] = "MISSING"

                if user_id_ext and hasattr(self, "save_audit_log_background"):
                    asyncio.create_task(
                        self.save_audit_log_background(
                            user_id_ext=user_id_ext,
                            source=source_str,
                            action=action_type,
                            details=details
                        )
                    )

            except Exception as log_err:
                logger.error(f"[Audit] Failed to log action: {log_err}")

            if status == "ERROR" and isinstance(result, Exception):
                raise result
            return result
        return wrapper
    return decorator


# -------------------------------------------------------------------------
# CLASE USER DATA MANAGER
# -------------------------------------------------------------------------
class UserDataManager:
    """
    Gestor de datos adaptado al esquema DBUser + UserDevice (One User, Many Devices).
    Retorna modelos Pydantic 'User'.
    """

    FAVORITE_TYPE_ORDER = {
        TransportType.METRO.value: 0,
        TransportType.BUS.value: 1,
        TransportType.TRAM.value: 2,
        TransportType.RODALIES.value: 3
    }

    def __init__(self):
        logger.info("Initializing UserDataManager with New DB Schema & Pydantic...")

    # ---------------------------------------------------------------------
    # MÉTODOS PRIVADOS (RESOLUCIÓN DE USUARIOS)
    # ---------------------------------------------------------------------

    async def _resolve_user_internal_id(self, session: AsyncSession, external_id: str, source: str) -> Optional[int]:
        if not external_id: return None

        if source == ClientType.TELEGRAM.value:
            stmt = select(DBUser.id).where(DBUser.telegram_id == str(external_id))
            res = await session.execute(stmt)
            return res.scalars().first()
        else:
            # Para Android, el external_id es el installation_id
            stmt = select(DBUserDevice.user_id).where(DBUserDevice.installation_id == str(external_id))
            res = await session.execute(stmt)
            return res.scalars().first()

    async def save_audit_log_background(self, user_id_ext, source, action, details):
        async with async_session_factory() as session:
            try:
                internal_id = await self._resolve_user_internal_id(session, user_id_ext, source)
                
                new_log = DBAuditLog(
                    user_id=internal_id,
                    client_source=source,
                    action=action,
                    details=details
                )
                session.add(new_log)
                await session.commit()
            except Exception as e:
                logger.error(f"[Audit] DB Write Failed: {e}")

    # ---------------------------
    # USERS & REGISTRATION
    # ---------------------------

    @audit_action(action_type="REGISTER_USER", params_args=["username"])
    async def register_user(self, client_source: ClientType, user_id: str, username: str, fcm_token: str = "") -> bool:
        async with async_session_factory() as session:
            try:
                db_user = None
                is_new = False
                final_username = username

                # Limpieza de nombre hardcodeado
                if client_source == ClientType.ANDROID and (not username or username == "android_user"):
                    final_username = None

                if client_source == ClientType.TELEGRAM:
                    stmt = select(DBUser).where(DBUser.telegram_id == str(user_id))
                    res = await session.execute(stmt)
                    db_user = res.scalars().first()
                else:
                    stmt = select(DBUser).join(DBUserDevice).where(DBUserDevice.installation_id == str(user_id))
                    res = await session.execute(stmt)
                    db_user = res.scalars().first()

                # CREAR SI NO EXISTE
                if not db_user:
                    is_new = True
                    if client_source == ClientType.TELEGRAM:
                        db_user = DBUser(
                            telegram_id=str(user_id),
                            username=username,
                            source=UserSource.TELEGRAM,
                            language="es"
                        )
                        session.add(db_user)
                        await session.flush()
                    else:
                        db_user = DBUser(
                            source=UserSource.ANDROID,
                            username=final_username,
                            language="es"
                        )
                        session.add(db_user)
                        await session.flush()
                        
                        new_device = DBUserDevice(
                            user_id=db_user.id,
                            installation_id=str(user_id),
                            fcm_token=fcm_token
                        )
                        session.add(new_device)
                else:
                    # ACTUALIZAR EXISTENTE
                    if final_username and db_user.username != final_username:
                        db_user.username = final_username                        
                    
                    if client_source == ClientType.ANDROID:
                        stmt_dev = select(DBUserDevice).where(DBUserDevice.installation_id == str(user_id))
                        res_dev = await session.execute(stmt_dev)
                        device = res_dev.scalars().first()
                        
                        if device:
                            if fcm_token and device.fcm_token != fcm_token:
                                device.fcm_token = fcm_token
                        else:
                            new_device = DBUserDevice(
                                user_id=db_user.id,
                                installation_id=str(user_id),
                                fcm_token=fcm_token
                            )
                            session.add(new_device)

                await session.commit()
                return is_new
            except Exception as e:
                logger.error(f"Error registering user {user_id}: {e}")
                await session.rollback()
                return False

    @audit_action(action_type="UPDATE_LANGUAGE", params_args=["new_language"]) # TODO: Adapt, only for TELEGRAM
    async def update_user_language(self, client_source: ClientType, user_id: str, new_language: str):
        async with async_session_factory() as session:
            source_str = client_source.value if hasattr(client_source, "value") else str(client_source)
            internal_id = await self._resolve_user_internal_id(session, str(user_id), source_str)
            if not internal_id: return False

            stmt = update(DBUser).where(DBUser.id == internal_id).values(language=new_language)
            await session.execute(stmt)
            await session.commit()
            return True

    async def get_user_language(self, client_source: ClientType, user_id: str) -> str: # TODO: Adapt, only for TELEGRAM
        async with async_session_factory() as session:
            source_str = client_source.value if hasattr(client_source, "value") else str(client_source)
            internal_id = await self._resolve_user_internal_id(session, str(user_id), source_str)
            if not internal_id: return "es"

            stmt = select(DBUser.language).where(DBUser.id == internal_id)
            result = await session.execute(stmt)
            lang = result.scalars().first()
            return lang if lang else "en"

    # ---------------------------
    # NOTIFICATIONS & LISTING
    # ---------------------------
    
    async def update_user_receive_notifications(self, client_source: str, user_id_ext: str, receive_notifications: bool) -> bool:
        async with async_session_factory() as session:
            internal_id = await self._resolve_user_internal_id(session, str(user_id_ext), str(client_source))
            if not internal_id:
                raise HTTPException(status_code=404, detail="User device not found")

            stmt = select(DBUser).where(DBUser.id == internal_id)
            result = await session.execute(stmt)
            user = result.scalar_one_or_none()

            if not user:
                raise HTTPException(status_code=404, detail="User profile not found")

            user.receive_notifications = receive_notifications
            await session.commit()
            return user.receive_notifications
        
    async def get_user_receive_notifications(self, client_source: str, user_id_ext: str) -> bool:
        async with async_session_factory() as session:
            internal_id = await self._resolve_user_internal_id(session, str(user_id_ext), str(client_source))
            if not internal_id: return True

            stmt = select(DBUser.receive_notifications).where(DBUser.id == internal_id)
            result = await session.execute(stmt)
            status = result.scalar()
            return status if status is not None else True

    # ---------------------------
    # FAVORITES
    # ---------------------------

    @audit_action(action_type="ADD_FAVORITE", params_args=["type", "item"])
    async def add_favorite(self, client_source: ClientType, user_id: str, type: str, item: FavoriteResponse):
        async with async_session_factory() as session:
            source_str = client_source.value if hasattr(client_source, "value") else str(client_source)
            internal_id = await self._resolve_user_internal_id(session, str(user_id), source_str)
            if not internal_id:
                logger.warning(f"Cannot add favorite: User {user_id} not found in DB")
                return False

            try:
                lat = item.coordinates[0] if item.coordinates and len(item.coordinates) > 0 else None
                lon = item.coordinates[1] if item.coordinates and len(item.coordinates) > 1 else None

                new_fav = DBFavorite(
                    user_id=internal_id,
                    transport_type=type.lower(),
                    station_code=item.station_code,
                    station_name=item.station_name,
                    station_group_code=item.station_group_code,
                    line_name=item.line_name,
                    line_name_with_emoji=item.line_name_with_emoji,
                    line_code=item.line_code,
                    latitude=lat,
                    longitude=lon
                )
                session.add(new_fav)
                await session.commit()
                return True
            except Exception as e:
                logger.error(f"Error adding favorite: {e}")
                return False

    @audit_action(action_type="REMOVE_FAVORITE", params_args=["type", "item_id"])
    async def remove_favorite(self, client_source: ClientType, user_id: str, type: str, item_id: str):
        async with async_session_factory() as session:
            source_str = client_source.value if hasattr(client_source, "value") else str(client_source)
            internal_id = await self._resolve_user_internal_id(session, str(user_id), source_str)
            if not internal_id: return False

            stmt = delete(DBFavorite).where(
                and_(
                    DBFavorite.user_id == internal_id,
                    DBFavorite.transport_type == type.lower(),
                    DBFavorite.station_code == str(item_id)
                )
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0
        
    async def get_user_cards(self, client_source: ClientType, user_id: str):
        async with async_session_factory() as session:
            source_str = client_source.value if hasattr(client_source, "value") else str(client_source)
            internal_id = await self._resolve_user_internal_id(session, str(user_id), source_str)
            if not internal_id: return False

            stmt = select(DBUserCard).where(DBUserCard.user_id == internal_id).order_by(DBUserCard.expiration_date.asc())
            result = await session.execute(stmt)
            db_cards = result.scalars().all()

            card_items = []
            for c in db_cards:
                card_items.append(self._to_domain_card(c))
            
            return sorted(
                card_items,
                key=lambda c: c.created_at
            )
        
    async def create_user_card(self, client_source: ClientType, user_id: str, item: CardCreate):
        async with async_session_factory() as session:
            source_str = client_source.value if hasattr(client_source, "value") else str(client_source)
            internal_id = await self._resolve_user_internal_id(session, str(user_id), source_str)
            if not internal_id: return False

            exp_date = item.expiration_date
            if exp_date.tzinfo is not None:
                exp_date = exp_date.astimezone(timezone.utc).replace(tzinfo=None)

            try:
                new_card = DBUserCard(
                    user_id=internal_id,
                    name=item.name,
                    expiration_date=exp_date
                )

                session.add(new_card)
                await session.commit()
                return True
            except Exception as e:
                logger.error(f"Error adding user card: {e}")
                return False
            
    async def remove_user_card(self, client_source: ClientType, user_id: str, item_id: int):
        async with async_session_factory() as session:
            source_str = client_source.value if hasattr(client_source, "value") else str(client_source)
            internal_id = await self._resolve_user_internal_id(session, str(user_id), source_str)
            if not internal_id: return False

            stmt = delete(DBUserCard).where(
                and_(
                    DBUserCard.user_id == internal_id,
                    DBUserCard.id == item_id
                )
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0
        
    async def update_favorite_alias(
        self, 
        client_source: ClientType, 
        user_id: str, 
        transport_type: str, 
        station_code: str, 
        new_alias: str
    ) -> bool:
        async with async_session_factory() as session:
            source_str = client_source.value if hasattr(client_source, "value") else str(client_source)
            internal_id = await self._resolve_user_internal_id(session, str(user_id), source_str)
            
            if not internal_id: 
                return False

            stmt = (
                update(DBFavorite)
                .where(
                    and_(
                        DBFavorite.user_id == internal_id,
                        DBFavorite.transport_type == transport_type.lower(),
                        DBFavorite.station_code == str(station_code)
                    )
                )
                .values(alias=new_alias)
            )
            
            result = await session.execute(stmt)
            await session.commit()
            
            return result.rowcount > 0

    @audit_action(action_type="GET_FAVORITES", params_args=[])
    async def get_favorites_by_user(self, client_source: ClientType, user_id: str) -> List[FavoriteResponse]:
        async with async_session_factory() as session:
            source_str = client_source.value if hasattr(client_source, "value") else str(client_source)
            internal_id = await self._resolve_user_internal_id(session, str(user_id), source_str)
            if not internal_id: return []

            stmt = select(DBFavorite).where(DBFavorite.user_id == internal_id)
            result = await session.execute(stmt)
            db_favs = result.scalars().all()

            fav_items = []
            for f in db_favs:
                fav_items.append(self._to_domain_favorite(f, str(user_id)))
            
            return sorted(
                fav_items,
                key=lambda f: self.FAVORITE_TYPE_ORDER.get(f.type, 999)
            )
        
    async def check_favorite_exists(
        self, 
        client_source: ClientType, 
        user_id: str, 
        transport_type: str, 
        item_id: str
    ) -> bool:
        """Verifica si un favorito existe sin traer todos los datos."""
        async with async_session_factory() as session:
            source_str = client_source.value if hasattr(client_source, "value") else str(client_source)
            internal_id = await self._resolve_user_internal_id(session, str(user_id), source_str)
            
            if not internal_id: 
                return False

            stmt = select(DBFavorite.id).where(
                and_(
                    DBFavorite.user_id == internal_id,
                    DBFavorite.transport_type == transport_type.lower(),
                    DBFavorite.station_code == str(item_id)
                )
            )
            result = await session.execute(stmt)
            return result.first() is not None

    async def get_active_users_with_favorites(self) -> List[tuple[User, List[FavoriteResponse]]]:
        """
        Obtiene usuarios con favoritos y sus tokens para notificaciones.
        Mapea al nuevo User Pydantic y asigna el FCM Token correcto a cada instancia.
        """
        async with async_session_factory() as session:
            stmt = (
                select(DBUser, DBFavorite, DBUserDevice.fcm_token)
                .join(DBFavorite, DBUser.id == DBFavorite.user_id)
                .join(DBUserDevice, DBUser.id == DBUserDevice.user_id)
                .where(DBUser.source == UserSource.ANDROID)
            )
            
            result = await session.execute(stmt)
            rows = result.all()
            
            grouped_data = {}

            for db_user, db_fav, token in rows:
                if not token: continue
                
                key = token 
                
                if key not in grouped_data:
                    domain_user = self._to_domain_user(db_user, fcm_token=token)
                    
                    grouped_data[key] = {
                        "user": domain_user,
                        "favorites": [],
                        "seen_favs": set()
                    }
                
                fav_unique = f"{db_fav.station_code}_{db_fav.transport_type}"
                if fav_unique not in grouped_data[key]["seen_favs"]:
                    grouped_data[key]["favorites"].append(
                        self._to_domain_favorite(db_fav, "system")
                    )
                    grouped_data[key]["seen_favs"].add(fav_unique)

            return [
                (data["user"], data["favorites"]) 
                for data in grouped_data.values()
            ]

    # ---------------------------
    # SEARCH HISTORY & ALERTS (Sin cambios)
    # ---------------------------
    async def register_search(self, query: str, client_source: ClientType, user_id: str):
        async with async_session_factory() as session:
            source_str = client_source.value if hasattr(client_source, "value") else str(client_source)
            internal_id = await self._resolve_user_internal_id(session, str(user_id), source_str)
            if internal_id:
                new_search = DBSearchHistory(user_id=internal_id, query=query)
                session.add(new_search)
                await session.commit()
                return 1
            return 0

    async def get_search_history(self, client_source: ClientType, user_id: str) -> List[str]:
        async with async_session_factory() as session:
            source_str = client_source.value if hasattr(client_source, "value") else str(client_source)
            internal_id = await self._resolve_user_internal_id(session, str(user_id), source_str)
            if not internal_id: return []

            stmt = (
                select(DBSearchHistory.query)
                .where(DBSearchHistory.user_id == internal_id)
                .group_by(DBSearchHistory.query)
                .order_by(func.max(DBSearchHistory.timestamp).desc())
                .limit(10)
            )
            result = await session.execute(stmt)
            return result.scalars().all()

    async def register_alert(self, transport_type: TransportType, api_alert: Alert):
        async with async_session_factory() as session:
            stmt = select(DBAlert).where(
                and_(
                    DBAlert.external_id == str(api_alert.id),
                    DBAlert.transport_type == transport_type.value
                )
            )
            result = await session.execute(stmt)
            if result.scalars().first(): return False

            new_incident = DBAlert(
                external_id=str(api_alert.id),
                transport_type=transport_type.value,
                begin_date=api_alert.begin_date,
                end_date=api_alert.end_date,
                status=api_alert.status,
                cause=api_alert.cause,
                publications=[pub.__dict__ for pub in api_alert.publications],
                affected_entities=[ent.__dict__ for ent in api_alert.affected_entities]
            )
            session.add(new_incident)
            await session.commit()
            return True

    async def get_alerts(self, only_active: bool = True) -> List[Alert]:
        async with async_session_factory() as session:
            stmt = select(DBAlert)
            if only_active:
                now = datetime.now()
                stmt = stmt.where((DBAlert.end_date == None) | (DBAlert.end_date > now))
            result = await session.execute(stmt)
            db_alerts = result.scalars().all()

            domain_alerts = []
            for a in db_alerts:
                pubs = [Publication(**p) for p in (a.publications or [])]
                ents = [AffectedEntity(**e) for e in (a.affected_entities or [])]
                domain_alerts.append(Alert(
                    id=str(a.external_id),
                    transport_type=TransportType(a.transport_type) if a.transport_type else None,
                    begin_date=a.begin_date,
                    end_date=a.end_date,
                    status=a.status,
                    cause=a.cause,
                    publications=pubs,
                    affected_entities=ents
                ))
            return domain_alerts
        
    async def has_notification_been_sent(self, user_id_ext: str, alert_id: str) -> bool:
        """
        Verifica si existe un log en DBNotificationLog para este usuario y alerta.
        """
        async with async_session_factory() as session:
            # Como user_id_ext puede ser un telegram_id o installation_id, primero resolvemos el ID interno
            # Truco: Probamos resolver como System o unknown, o iteramos sources. 
            # Para simplificar, buscamos el user primero por cualquiera de los medios.
            
            # Buscamos el ID interno del usuario
            # (Puedes optimizar esto si pasas el internal_id desde el servicio, 
            #  pero user_id_ext es lo que tenemos en el objeto User del dominio)
            
            # Intento 1: Es numérico (Telegram o ID interno viejo)
            stmt_user = select(DBUser.id).where(
                (DBUser.telegram_id == str(user_id_ext)) | 
                (DBUser.id == int(user_id_ext) if user_id_ext.isdigit() else False)
            )
            res = await session.execute(stmt_user)
            internal_id = res.scalars().first()
            
            if not internal_id:
                # Intento 2: Es Installation ID (Android)
                stmt_dev = select(DBUserDevice.user_id).where(DBUserDevice.installation_id == str(user_id_ext))
                res = await session.execute(stmt_dev)
                internal_id = res.scalars().first()
            
            if not internal_id: return False

            # Consulta eficiente al log
            stmt_log = select(DBNotificationLog).where(
                and_(
                    DBNotificationLog.user_id == internal_id,
                    DBNotificationLog.alert_id == str(alert_id)
                )
            )
            result = await session.execute(stmt_log)
            return result.scalar_one_or_none() is not None

    async def log_notification_sent(self, user_id: str, alert_id: str, client_source: ClientType):
        """
        Crea el registro en DBNotificationLog.
        Es capaz de manejar tanto external_ids (InstallationID/TelegramID) como internal_ids ("1").
        """
        async with async_session_factory() as session:
            internal_id = None
            source_str = client_source.value if hasattr(client_source, "value") else str(client_source)

            internal_id = await self._resolve_user_internal_id(session, str(user_id), source_str)
            if not internal_id and str(user_id).isdigit():
                stmt = select(DBUser.id).where(DBUser.id == int(user_id))
                res = await session.execute(stmt)
                internal_id = res.scalars().first()

            if internal_id:
                stmt_check = select(DBNotificationLog).where(
                    and_(
                        DBNotificationLog.user_id == internal_id,
                        DBNotificationLog.alert_id == str(alert_id)
                    )
                )
                existing = await session.execute(stmt_check)
                
                if not existing.scalar_one_or_none():
                    new_log = DBNotificationLog(
                        user_id=internal_id,
                        alert_id=str(alert_id)
                    )
                    session.add(new_log)
                    await session.commit()
            else:
                logger.warning(f"[LogNotification] No se pudo encontrar usuario para user_id='{user_id}' (Source: {source_str})")

    # ---------------------------
    # HELPERS
    # ---------------------------
    def _to_domain_user(self, db_user: DBUser, fcm_token: str = "") -> User:
        """
        Convierte DBUser (SQLAlchemy) a User (Pydantic).
        Calcula el 'auth_provider' basado en los datos disponibles.
        """
        # Determinar el proveedor de autenticación
        auth_provider = "device"
        if db_user.source == UserSource.TELEGRAM:
            auth_provider = "telegram"
        elif db_user.firebase_uid:
            auth_provider = "google"

        # Identificador principal para el objeto de dominio
        # Si es Telegram, usamos telegram_id. Si es Android, usamos ID interno (ya que no hay un solo install_id)
        user_id_str = db_user.telegram_id if db_user.telegram_id else str(db_user.id)

        return User(
            user_id=user_id_str,
            username=db_user.username,
            created_at=db_user.created_at,
            language=db_user.language or "es",
            receive_notifications=db_user.receive_notifications if db_user.receive_notifications is not None else True,
            
            # Nuevos campos
            email=db_user.email,
            firebase_uid=db_user.firebase_uid,
            photo_url=db_user.photo_url,
            auth_provider=auth_provider,
            
            fcm_token=fcm_token,
            already_notified=[]
        )

    def _to_domain_favorite(self, f: DBFavorite, user_id_ext: str) -> FavoriteResponse:
        return FavoriteResponse(
            user_id=str(user_id_ext),
            type=f.transport_type,
            station_code=f.station_code,
            station_name=f.station_name,
            station_group_code=f.station_group_code or "",
            line_name=f.line_name or "",
            line_name_with_emoji=f.line_name_with_emoji or "",
            line_code=f.line_code or "",
            coordinates=[f.latitude or 0, f.longitude or 0],
            alias=f.alias
        )
    
    def _to_domain_card(self, c: DBUserCard) -> CardResponse:
        return CardResponse(
            id=c.id,
            created_at=c.created_at,
            expiration_date=c.expiration_date,
            name=c.name
        )