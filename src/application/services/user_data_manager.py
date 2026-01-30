import asyncio
import inspect
import logging
from typing import List, Optional
from datetime import datetime, timezone
from functools import wraps

# SQLAlchemy & DB
from sqlalchemy import select, delete, update, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from src.infrastructure.database.repositories.user_repository import UserRepository
from src.domain.models.common.user_settings import UserSettingsResponse, UserSettingsUpdate
from src.domain.models.common.card import CardCreate, CardResponse, CardUpdate
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
    DBUserSettings, 
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
            stmt_user = select(DBUser.id).where(DBUser.firebase_uid == str(external_id))
            res_user = await session.execute(stmt_user)
            user_id = res_user.scalars().first()
            
            if user_id:
                return user_id
            
            stmt_device = (
                select(DBUserDevice.user_id)
                .where(DBUserDevice.installation_id == str(external_id))
                .order_by(DBUserDevice.id.desc())
            )
            res_device = await session.execute(stmt_device)
            return res_device.scalars().first()
    
    async def get_user_id_by_google_uid(self, google_uid: str) -> Optional[int]:
        async with async_session_factory() as session:
            stmt = select(DBUser.id).where(DBUser.firebase_uid == str(google_uid))
            res = await session.execute(stmt)
            return res.scalars().first()

    async def get_user_id_by_installation_id(self, installation_id: str) -> Optional[int]:
        async with async_session_factory() as session:
            stmt = (
                select(DBUserDevice.user_id)
                .where(DBUserDevice.installation_id == str(installation_id))
                .order_by(DBUserDevice.id.desc()) 
            )
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

    @audit_action(action_type="REGISTER_DEVICE", params_args=["username"])
    async def register_device(self, client_source: ClientType, installation_id: str, username: str, fcm_token: str = "") -> bool:
        user_repo = UserRepository(async_session_factory)
        async with async_session_factory() as session:
            final_username = username
            if client_source == ClientType.ANDROID and (not username or username == "android_user"):
                final_username = None

            db_user = await user_repo.get_user_by_installation_id(str(installation_id))

            is_new = False

            if not db_user:
                is_new = True
                new_user = DBUser(
                    source=UserSource.ANDROID,
                    username=final_username
                )
                
                new_device = DBUserDevice(
                    installation_id=str(installation_id),
                    fcm_token=fcm_token
                )

                await user_repo.create_with_device(new_user, new_device)

            else:
                if final_username and db_user.username != final_username:
                    db_user.username = final_username
                    await user_repo.update(db_user)
                
                if client_source == ClientType.ANDROID:
                    await user_repo.register_device_entry(
                        user_id=db_user.id,
                        installation_id=str(installation_id),
                        fcm_token=fcm_token
                    )

            return is_new
        
    # ---------------------------
    # FAVORITES
    # ---------------------------

    @audit_action(action_type="ADD_FAVORITE", params_args=["type", "item"])
    async def add_favorite(self, user_id: str, type: str, item: FavoriteResponse):
        async with async_session_factory() as session:
            if not user_id:
                logger.warning(f"Cannot add favorite: User {user_id} not found in DB")
                return False

            try:
                lat = item.coordinates[0] if item.coordinates and len(item.coordinates) > 0 else None
                lon = item.coordinates[1] if item.coordinates and len(item.coordinates) > 1 else None

                new_fav = DBFavorite(
                    user_id=user_id,
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
    async def remove_favorite(self, user_id: str, type: str, item_id: str):
        async with async_session_factory() as session:
            if not user_id: return False

            stmt = delete(DBFavorite).where(
                and_(
                    DBFavorite.user_id == user_id,
                    DBFavorite.transport_type == type.lower(),
                    DBFavorite.station_code == str(item_id)
                )
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0
    
    async def update_favorite_alias(
        self,
        user_id: str, 
        transport_type: str, 
        station_code: str, 
        new_alias: str
    ) -> bool:
        async with async_session_factory() as session:
            
            if not user_id: 
                return False

            stmt = (
                update(DBFavorite)
                .where(
                    and_(
                        DBFavorite.user_id == user_id,
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
    async def get_favorites_by_user(self, user_id: str) -> List[FavoriteResponse]:
        async with async_session_factory() as session:
            if not user_id: return []

            stmt = select(DBFavorite).where(DBFavorite.user_id == user_id)
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
        user_id: str, 
        transport_type: str, 
        item_id: str
    ) -> bool:
        """Verifica si un favorito existe sin traer todos los datos."""
        async with async_session_factory() as session:

            if not user_id: 
                return False

            stmt = select(DBFavorite.id).where(
                and_(
                    DBFavorite.user_id == user_id,
                    DBFavorite.transport_type == transport_type.lower(),
                    DBFavorite.station_code == str(item_id)
                )
            )
            result = await session.execute(stmt)
            return result.first() is not None

    async def get_active_users_with_favorites(self) -> List[tuple[User, List[FavoriteResponse]]]:
        async with async_session_factory() as session:
            stmt = (
                select(DBUser, DBFavorite, DBUserDevice.fcm_token, DBUserSettings)
                .join(DBFavorite, DBUser.id == DBFavorite.user_id)
                .join(DBUserDevice, DBUser.id == DBUserDevice.user_id)
                .outerjoin(DBUserSettings, DBUser.id == DBUserSettings.user_id)
                .where(DBUser.source == UserSource.ANDROID)
            )
            
            result = await session.execute(stmt)
            rows = result.all()
            
            grouped_data = {}

            for db_user, db_fav, token, db_settings in rows:
                if not token: continue
                notifications_enabled = True
                if db_settings is not None:
                    notifications_enabled = db_settings.general_notifications_enabled
                
                if not notifications_enabled:
                    continue
                
                key = token 
                
                if key not in grouped_data:
                    domain_user = self._to_domain_user(db_user, fcm_token=token, db_settings=db_settings)

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
    # CARDS
    # ---------------------------
    async def get_user_cards(self, user_id: str):
        async with async_session_factory() as session:
            if not user_id: return []

            stmt = select(DBUserCard).where(DBUserCard.user_id == user_id).order_by(DBUserCard.expiration_date.asc())
            result = await session.execute(stmt)
            db_cards = result.scalars().all()

            card_items = []
            for c in db_cards:
                card_items.append(self._to_domain_card(c))
            
            return sorted(
                card_items,
                key=lambda c: c.created_at
            )
        
    async def create_user_card(self, user_id: str, item: CardCreate):
        async with async_session_factory() as session:
            if not user_id: return False

            exp_date = item.expiration_date
            if exp_date.tzinfo is not None:
                exp_date = exp_date.astimezone(timezone.utc).replace(tzinfo=None)

            try:
                new_card = DBUserCard(
                    user_id=user_id,
                    name=item.name,
                    expiration_date=exp_date
                )

                session.add(new_card)
                await session.commit()
                return True
            except Exception as e:
                logger.error(f"Error adding user card: {e}")
                return False
            
    async def update_user_card(self, user_id: str, item: CardUpdate) -> bool:
        async with async_session_factory() as session:
            
            if not user_id: 
                return False

            stmt = select(DBUserCard).where(DBUserCard.user_id == user_id).filter(DBUserCard.id == item.id)
            result = await session.execute(stmt)
            db_card = result.scalars().first()

            if not db_card:
                return False
            
            exp_date = item.expiration_date
            if item.expiration_date.tzinfo is not None:
                item.expiration_date = exp_date.astimezone(timezone.utc).replace(tzinfo=None)

            update_data = item.model_dump(exclude_unset=True)

            for key, value in update_data.items():
                setattr(db_card, key, value)
                
            await session.commit()

            return True
            
    async def remove_user_card(self, user_id: str, item_id: int):
        async with async_session_factory() as session:
            if not user_id: return False

            stmt = delete(DBUserCard).where(
                and_(
                    DBUserCard.user_id == user_id,
                    DBUserCard.id == item_id
                )
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0
        

    # ---------------------------
    # SETTINGS
    # ---------------------------
    async def get_user_settings(self, user_id: str) -> UserSettingsResponse:
        async with async_session_factory() as session:
            
            if not user_id:
                return UserSettingsResponse()

            stmt = select(DBUserSettings).where(DBUserSettings.user_id == user_id)
            result = await session.execute(stmt)
            config = result.scalars().first()

            if not config:
                return UserSettingsResponse(
                    language="es",
                    theme_mode="system",
                    general_notifications_enabled=True,
                    card_alerts_enabled=True,
                    card_alert_days_before=3,
                    card_alert_hour=9
                )

            return UserSettingsResponse(
                language=config.language,
                theme_mode=config.theme_mode,
                general_notifications_enabled=config.general_notifications_enabled,
                
                card_alerts_enabled=config.card_alerts_enabled,
                card_alert_days_before=config.card_alert_days_before,
                card_alert_hour=config.card_alert_hour
            )

    async def update_user_settings(self, user_id: str, item: UserSettingsUpdate) -> bool:
        async with async_session_factory() as session:
            
            if not user_id: 
                return False

            stmt = select(DBUserSettings).where(DBUserSettings.user_id == user_id)
            result = await session.execute(stmt)
            db_settings = result.scalars().first()

            if not db_settings:
                db_settings = DBUserSettings(user_id=user_id)
                session.add(db_settings)

            update_data = item.model_dump(exclude_unset=True)

            for key, value in update_data.items():
                setattr(db_settings, key, value)
                
            await session.commit()

            return True
        

    # ---------------------------
    # SEARCH HISTORY & ALERTS (Sin cambios)
    # ---------------------------
    async def register_search(self, query: str, user_id: str):
        async with async_session_factory() as session:
            if user_id:
                new_search = DBSearchHistory(user_id=user_id, query=query)
                session.add(new_search)
                await session.commit()
                return 1
            return 0

    async def get_search_history(self, user_id: str) -> List[str]:
        async with async_session_factory() as session:
            if not user_id: return []

            stmt = (
                select(DBSearchHistory.query)
                .where(DBSearchHistory.user_id == user_id)
                .group_by(DBSearchHistory.query)
                .order_by(func.max(DBSearchHistory.timestamp).desc())
                .limit(10)
            )
            result = await session.execute(stmt)
            return result.scalars().all()


    # ---------------------------
    # ALERTS
    # ---------------------------
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
        
    async def has_notification_been_sent(self, user_id_db: str, alert_id: str) -> bool:
        if not user_id_db.isdigit():
            return False

        async with async_session_factory() as session:
            stmt_log = select(DBNotificationLog).where(
                and_(
                    DBNotificationLog.user_id == int(user_id_db),
                    DBNotificationLog.alert_id == str(alert_id)
                )
            )
            
            result = await session.execute(stmt_log)
            
            return result.scalar_one_or_none() is not None

    async def log_notification_sent(self, user_id: str, alert_id: str):
        """
        Crea el registro en DBNotificationLog.
        Es capaz de manejar tanto external_ids (InstallationID/TelegramID) como internal_ids ("1").
        """
        async with async_session_factory() as session:
            if str(user_id).isdigit():
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
                logger.warning(f"[LogNotification] No se pudo encontrar usuario para user_id='{user_id}'")

    # ---------------------------
    # HELPERS
    # ---------------------------
    def _to_domain_user(self, db_user: DBUser, fcm_token: str = "", db_settings: DBUserSettings | None = None) -> User:
        auth_provider = "device"
        if db_user.source == UserSource.TELEGRAM:
            auth_provider = "telegram"
        elif db_user.firebase_uid:
            auth_provider = "google"
            
        user_id_str = db_user.telegram_id if db_user.telegram_id else str(db_user.id)

        settings_dto = None
        if db_settings:
            settings_dto = UserSettingsResponse(
                general_notifications_enabled=db_settings.general_notifications_enabled,
                language=db_settings.language, 
                theme_mode=db_settings.theme_mode,
                card_alerts_enabled=db_settings.card_alerts_enabled,
                card_alert_hour=db_settings.card_alert_hour,
                card_alert_days_before=db_settings.card_alert_days_before
            )

        return User(
            user_id=user_id_str,
            username=db_user.username,
            created_at=db_user.created_at,
            
            email=db_user.email,
            firebase_uid=db_user.firebase_uid,
            photo_url=db_user.photo_url,
            auth_provider=auth_provider,
            fcm_token=fcm_token,
            
            settings=settings_dto 
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