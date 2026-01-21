import asyncio
import inspect
import logging
from typing import List, Optional, Union
from datetime import datetime
from functools import wraps

# SQLAlchemy & DB
from sqlalchemy import select, delete, update, and_, func, or_
from sqlalchemy.ext.asyncio import AsyncSession
from src.infrastructure.database.database import async_session_factory
from src.domain.enums.clients import ClientType
from src.domain.enums.transport_type import TransportType

# Domain Models
from src.domain.models.common.user import User
from src.domain.models.common.alert import Alert, AffectedEntity, Publication

# Database Models (El esquema nuevo)
from src.domain.schemas.models import (
    DBUser, 
    UserDevice as DBUserDevice,
    Favorite as DBFavorite, 
    Alert as DBAlert, 
    AuditLog as DBAuditLog, 
    DBSearchHistory,
    UserSource
)

# Domain Schemas
from src.domain.schemas.favorite import FavoriteResponse

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# DECORADOR DE AUDITORÍA
# -------------------------------------------------------------------------
def audit_action(action_type: str, params_args: list = None):
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            # 1. Ejecución inmediata
            try:
                result = await func(self, *args, **kwargs)
                status = "SUCCESS"
                error_info = None
            except Exception as e:
                result = e
                status = "ERROR"
                error_info = str(e)

            # 2. Extracción de datos
            try:
                sig = inspect.signature(func)
                bound_args = sig.bind(self, *args, **kwargs)
                bound_args.apply_defaults()
                func_args = bound_args.arguments

                # Extracción segura de Source
                raw_source = func_args.get("client_source", "UNKNOWN")
                if hasattr(raw_source, "value"):
                    source_str = str(raw_source.value)
                elif isinstance(raw_source, ClientType):
                    source_str = raw_source.value
                else:
                    source_str = str(raw_source)

                # User ID externo (Installation ID o Telegram ID)
                raw_user_id = func_args.get("user_id")
                user_id_ext = str(raw_user_id) if raw_user_id is not None else None

                details = {"params": {}, "status": status}
                if error_info: details["error"] = error_info

                if params_args:
                    for param_name in params_args:
                        if param_name in func_args:
                            val = func_args[param_name]
                            if hasattr(val, "value"): details["params"][param_name] = str(val.value)
                            elif hasattr(val, "model_dump_json"): details["params"][param_name] = val.model_dump_json()
                            elif hasattr(val, "dict"): details["params"][param_name] = str(val.dict())
                            else: details["params"][param_name] = str(val)

                # 3. Guardado en background
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
    """

    FAVORITE_TYPE_ORDER = {
        TransportType.METRO.value: 0,
        TransportType.BUS.value: 1,
        TransportType.TRAM.value: 2,
        TransportType.RODALIES.value: 3
    }

    def __init__(self):
        logger.info("Initializing UserDataManager with New DB Schema...")

    # ---------------------------------------------------------------------
    # MÉTODOS PRIVADOS (RESOLUCIÓN DE USUARIOS)
    # ---------------------------------------------------------------------

    async def _resolve_user_internal_id(self, session: AsyncSession, external_id: str, source: str) -> Optional[int]:
        """
        Método CRÍTICO: Decide cómo buscar al usuario según el origen.
        - Si es TELEGRAM -> Busca en DBUser.telegram_id
        - Si es ANDROID -> Busca en UserDevice.installation_id y devuelve el user_id padre.
        """
        if not external_id: return None

        if source == ClientType.TELEGRAM.value:
            # Búsqueda directa en tabla users
            stmt = select(DBUser.id).where(DBUser.telegram_id == str(external_id))
            res = await session.execute(stmt)
            return res.scalars().first()
        else:
            # Asumimos Android/App: external_id es el 'installation_id' (UUID)
            # Buscamos en la tabla de dispositivos para encontrar al dueño
            stmt = select(DBUserDevice.user_id).where(DBUserDevice.installation_id == str(external_id))
            res = await session.execute(stmt)
            return res.scalars().first()

    async def save_audit_log_background(self, user_id_ext, source, action, details):
        """Guarda log resolviendo correctamente quién es el usuario."""
        async with async_session_factory() as session:
            try:
                # Reutilizamos la lógica de resolución
                internal_id = await self._resolve_user_internal_id(session, user_id_ext, source)
                
                new_log = DBAuditLog(
                    user_id=internal_id, # Puede ser None si el usuario falló al registrarse
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
        """
        Registra usuario dependiendo de la fuente.
        - Android: Crea DBUser + UserDevice (usando user_id como installation_id).
        - Telegram: Crea DBUser con telegram_id.
        """
        async with async_session_factory() as session:
            try:
                db_user = None
                is_new = False
                source_val = client_source.value

                # 1. BUSCAR USUARIO EXISTENTE
                if client_source == ClientType.TELEGRAM:
                    stmt = select(DBUser).where(DBUser.telegram_id == str(user_id))
                    res = await session.execute(stmt)
                    db_user = res.scalars().first()
                else:
                    # Android: Buscamos por installation_id en dispositivos
                    stmt = select(DBUser).join(DBUserDevice).where(DBUserDevice.installation_id == str(user_id))
                    res = await session.execute(stmt)
                    db_user = res.scalars().first()

                # 2. CREAR SI NO EXISTE
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
                        await session.flush() # Para tener ID
                    else:
                        # Android Nuevo: Usuario + Dispositivo
                        db_user = DBUser(
                            source=UserSource.ANDROID,
                            username=username,
                            language="es"
                        )
                        session.add(db_user)
                        await session.flush()
                        
                        # Creamos el dispositivo vinculado
                        new_device = DBUserDevice(
                            user_id=db_user.id,
                            installation_id=str(user_id),
                            fcm_token=fcm_token
                        )
                        session.add(new_device)
                else:
                    # 3. ACTUALIZAR EXISTENTE
                    if username:
                        db_user.username = username
                    
                    # Si es Android y ya existe el usuario, verificamos si el dispositivo existe
                    if client_source != ClientType.TELEGRAM:
                        # Buscamos este dispositivo específico
                        stmt_dev = select(DBUserDevice).where(DBUserDevice.installation_id == str(user_id))
                        res_dev = await session.execute(stmt_dev)
                        device = res_dev.scalars().first()
                        
                        if device:
                            # Actualizamos token si ha cambiado
                            if fcm_token and device.fcm_token != fcm_token:
                                device.fcm_token = fcm_token
                        else:
                            # Caso raro: Usuario existe pero este dispositivo no (Login en móvil nuevo)
                            # Nota: Normalmente esto se maneja en el endpoint de Login, pero por seguridad:
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

    @audit_action(action_type="UPDATE_LANGUAGE", params_args=["new_language"])
    async def update_user_language(self, client_source: ClientType, user_id: str, new_language: str):
        async with async_session_factory() as session:
            internal_id = await self._resolve_user_internal_id(session, str(user_id), client_source.value)
            if not internal_id: return False

            stmt = update(DBUser).where(DBUser.id == internal_id).values(language=new_language)
            await session.execute(stmt)
            await session.commit()
            return True

    async def get_user_language(self, client_source: ClientType, user_id: str) -> str:
        async with async_session_factory() as session:
            internal_id = await self._resolve_user_internal_id(session, str(user_id), client_source.value)
            if not internal_id: return "es"

            stmt = select(DBUser.language).where(DBUser.id == internal_id)
            result = await session.execute(stmt)
            lang = result.scalars().first()
            return lang if lang else "en"

    # ---------------------------
    # NOTIFICATIONS & LISTING
    # ---------------------------

    @audit_action(action_type="GET_ALL_USERS", params_args=[])
    async def get_users(self, client_source: ClientType = ClientType.SYSTEM) -> List[User]:
        """Devuelve usuarios. Nota: Para Android devuelve el installation_id como user_id para mantener compatibilidad."""
        async with async_session_factory() as session:
            # Traemos usuario y sus dispositivos
            stmt = select(DBUser)
            result = await session.execute(stmt)
            db_users = result.scalars().all()
            
            domain_users = []
            # Nota: Esto es una simplificación. Si un usuario tiene 3 dispositivos, 
            # esta lógica básica devuelve al usuario base. 
            # Ajustar según necesidad de negocio.
            for u in db_users:
                domain_users.append(User(
                    user_id=u.telegram_id if u.telegram_id else str(u.id), # Fallback ID
                    username=u.username,
                    created_at=u.created_at,
                    language=u.language,
                    receive_notifications=True, # Asumimos True si no hay campo
                    already_notified=[],
                    fcm_token="" # No cargamos tokens en masa por seguridad
                ))
            return domain_users

    @audit_action(action_type="GET_USER_RECEIVE_NOTIFICATIONS", params_args=[])
    async def get_user_receive_notifications(self, client_source: ClientType, user_id: str) -> bool:
        # En el nuevo modelo no tenías campo receive_notifications en DBUser, 
        # asumimos True o añádelo a DBUser si lo necesitas.
        return True 

    # ---------------------------
    # FAVORITES
    # ---------------------------

    @audit_action(action_type="ADD_FAVORITE", params_args=["type", "item"])
    async def add_favorite(self, client_source: ClientType, user_id: str, type: str, item: FavoriteResponse):
        async with async_session_factory() as session:
            internal_id = await self._resolve_user_internal_id(session, str(user_id), client_source.value)
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
            internal_id = await self._resolve_user_internal_id(session, str(user_id), client_source.value)
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

    async def get_active_users_with_favorites(self) -> List[tuple[User, List[FavoriteResponse]]]:
        """
        Obtiene usuarios con favoritos y sus tokens para notificaciones.
        CRÍTICO: Un usuario puede tener múltiples dispositivos, así que esto
        debería devolver tokens, no solo usuarios.
        """
        async with async_session_factory() as session:
            # Join Users -> Favorites AND Users -> Devices
            # Solo usuarios de Android que tengan dispositivos registrados
            stmt = (
                select(DBUser, DBFavorite, DBUserDevice.fcm_token)
                .join(DBFavorite, DBUser.id == DBFavorite.user_id)
                .join(DBUserDevice, DBUser.id == DBUserDevice.user_id) # Inner join, solo si tiene dispositivo
                .where(DBUser.source == UserSource.ANDROID)
            )
            
            result = await session.execute(stmt)
            rows = result.all()
            
            # Agrupamos por TOKEN de dispositivo (porque enviamos push al token, no al usuario abstracto)
            # Si un usuario tiene 2 móviles, procesaremos 2 entradas aquí.
            grouped_data = {}

            for db_user, db_fav, token in rows:
                if not token: continue
                
                # Usamos el token como clave única de envío
                key = token 
                
                if key not in grouped_data:
                    domain_user = self._to_domain_user(db_user)
                    domain_user.fcm_token = token
                    
                    grouped_data[key] = {
                        "user": domain_user,
                        "favorites": [],
                        "seen_favs": set()
                    }
                
                # Evitar duplicados de favoritos
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
    # SEARCH HISTORY
    # ---------------------------
    async def register_search(self, query: str, client_source: ClientType, user_id: str):
        async with async_session_factory() as session:
            internal_id = await self._resolve_user_internal_id(session, str(user_id), client_source.value)
            if internal_id:
                new_search = DBSearchHistory(user_id=internal_id, query=query)
                session.add(new_search)
                await session.commit()
                return 1
            return 0

    async def get_search_history(self, client_source: ClientType, user_id: str) -> List[str]:
        async with async_session_factory() as session:
            internal_id = await self._resolve_user_internal_id(session, str(user_id), client_source.value)
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

    # ---------------------------
    # ALERTS (Service Incidents) - NO CAMBIA
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

    # ---------------------------
    # HELPERS
    # ---------------------------
    def _to_domain_user(self, db_user: DBUser) -> User:
        # User Domain Model necesita mapeo
        return User(
            user_id=str(db_user.id), # Internal ID mapping
            username=db_user.username,
            created_at=db_user.created_at,
            language=db_user.language,
            receive_notifications=True,
            already_notified=[],
            fcm_token=""
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
            coordinates=[f.latitude or 0, f.longitude or 0]
        )