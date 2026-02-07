from __future__ import annotations
import hashlib
from typing import TYPE_CHECKING, List
from datetime import datetime, timedelta

import asyncio
import html
import os

from firebase_admin import messaging

from src.domain.enums.transport_type import TransportType
from src.infrastructure.database.repositories.alerts_repository import AlertsRepository
from src.domain.models.common.alert import AffectedEntity, Alert, Publication
from src.domain.models.common.user import User
from src.domain.schemas.favorite import FavoriteResponse
from src.application.services.user_data_manager import UserDataManager
from src.infrastructure.database.database import async_session_factory
from src.core.logger import logger

class AlertsService:
    def __init__(self, user_data_manager: UserDataManager, interval: int = 300):
        self.user_data_manager = user_data_manager
        
        env_interval = os.getenv("ALERTS_SERVICE_INTERVAL")
        self.interval = int(env_interval) if env_interval else interval
        self.alerts_repository = AlertsRepository(async_session_factory)
        
        self._running = False
        self._task = None
        self._semaphore = asyncio.Semaphore(10)
        self._last_card_check_hour = -1

    async def start(self):
        if self._running:
            logger.warning("⚠️ DEBUG: El servicio YA estaba corriendo.")
            return

        self._running = True
        try:
            self._task = asyncio.create_task(self.scheduler())
            logger.info(f"🚀 Alerts Service started. Interval: {self.interval}s")
        except Exception as e:
            logger.error(f"❌ DEBUG: Error al crear la tarea: {e}")

    async def stop(self):
        logger.info("🛑 Stopping Alerts Service...")
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def send_push_notification(self, fcm_token: str, title: str, body: str, data: dict = None):
        if not fcm_token: return None
        try:
            title = html.unescape(title)
            body = html.unescape(body)
            
            message = messaging.Message(
                notification=messaging.Notification(title=title, body=body),
                data=data or {},
                token=fcm_token,
            )
            
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(None, messaging.send, message)
            return response
        except Exception as e:
            logger.error(f"Error sending push to {fcm_token[:10]}...: {e}")
            return None

    # --- LÓGICA DE INCIDENCIAS DE TRANSPORTE ---

    async def _notify_user(self, user: User, alert: Alert):
        async with self._semaphore:            
            already_sent = await self.user_data_manager.has_notification_been_sent(user.user_id, alert.id)
            if already_sent:
                return

            try:
                if user.fcm_token:
                    logger.info(f"🔔 Sending INCIDENT PUSH to User:{user.user_id} - Alert {alert.id}")
                    title, lineas_summary, estaciones_summary, description, push_body = alert._get_alert_content()
                    await self.send_push_notification(
                        user.fcm_token,
                        title="BCN Transit | Incidencia",
                        body=push_body,
                        data={
                            "alert_id": str(alert.id),
                            "type": "incident",
                            "full_text": description
                        }
                    )
                    await self.user_data_manager.log_notification_sent(user.user_id, alert.id)
            except Exception as e:
                logger.error(f"Failed to notify user {user.user_id}: {e}")

    # --- NUEVA LÓGICA DE CADUCIDAD DE TARJETAS ---

    async def _notify_card_expiration(self, user: User, cards: list, days_left: int):
        """Notifica la caducidad de una o varias tarjetas."""
        async with self._semaphore:
            now_str = datetime.now().strftime("%Y%m%d")
            token_bytes = user.fcm_token.encode('utf-8')
            token_hash = token_hash = hashlib.sha256(token_bytes).hexdigest()[:12]
            alert_id = f"CARD_EXP_{user.user_id}_{cards[0].id}_{now_str}_{token_hash}"
            
            already_sent = await self.user_data_manager.has_notification_been_sent(user.user_id, alert_id)
            if already_sent:
                return

            try:
                if user.fcm_token:
                    lang = user.settings.language if user.settings else "es"
                    card_names = ", ".join([c.name for c in cards])
                    
                    title = "⚠️ Tarjeta próxima a caducar" if lang == "es" else "⚠️ Card expiring soon"
                    body = (f"Tu tarjeta '{card_names}' caduca en {days_left} días." 
                            if lang == "es" else 
                            f"Your card '{card_names}' expires in {days_left} days.")

                    logger.info(f"💳 Sending CARD PUSH to User:{user.user_id} - {card_names}")
                    await self.send_push_notification(
                        user.fcm_token,
                        title=title,
                        body=body,
                        data={"type": "card_expiration"}
                    )
                    await self.user_data_manager.log_notification_sent(user.user_id, alert_id)
            except Exception as e:
                logger.error(f"Failed card notification for user {user.user_id}: {e}")

    async def check_card_expirations(self):
        """Busca y notifica tarjetas que caducan según la configuración del usuario."""
        now = datetime.now()
        current_hour = now.hour

        try:
            users_data = await self.user_data_manager.get_users_for_card_alerts(current_hour)
            if not users_data: return

            tasks = []
            for user, settings in users_data:
                target_date = now.date() + timedelta(days=settings.card_alert_days_before)
                expiring_cards = await self.user_data_manager.get_user_cards_expiring_on(int(user.user_id), target_date)
                
                if expiring_cards:
                    tasks.append(self._notify_card_expiration(user, expiring_cards, settings.card_alert_days_before))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"❌ Error checking card expirations: {e}")

    async def check_new_alerts(self):
        try:
            db_alerts = await self.alerts_repository.get_active_alerts()
            if not db_alerts:
                return

            active_recent_alerts: List[Alert] = []
            now = datetime.now()
            relevance_threshold = now - timedelta(hours=24)

            for a in db_alerts:
                if a.begin_date and a.begin_date < relevance_threshold:
                    continue

                pubs = [Publication(**p) for p in (a.publications or [])]
                ents = [AffectedEntity(**e) for e in (a.affected_entities or [])]
                
                active_recent_alerts.append(Alert(
                    id=str(a.external_id),
                    transport_type=TransportType(a.transport_type) if a.transport_type else None,
                    begin_date=a.begin_date,
                    end_date=a.end_date,
                    status=a.status,
                    cause=a.cause,
                    publications=pubs,
                    affected_entities=ents
                ))

            if not active_recent_alerts:
                logger.info("ℹ️ No hay alertas recientes para notificar.")
                return

            users_data = await self.user_data_manager.get_active_users_with_favorites()
            if not users_data:
                return

            logger.info(f"🔎 Checking {len(active_recent_alerts)} recent alerts for {len(users_data)} users...")

            tasks = []
            for user, favorites in users_data:
                notifications_enabled = user.settings.general_notifications_enabled if user.settings else True
                if not notifications_enabled:
                    continue

                for alert in active_recent_alerts:
                    if self._is_alert_relevant_for_user(alert, favorites):
                        tasks.append(self._notify_user(user, alert))

            if tasks:
                logger.info(f"📨 Dispatching {len(tasks)} potential notifications...")
                await asyncio.gather(*tasks, return_exceptions=True)
            else:
                logger.info("📨 No hay nuevas notificaciones relevantes para enviar.")

        except Exception as e:
            logger.exception(f"❌ Error crítico en check_new_alerts: {e}")

    def _is_alert_relevant_for_user(self, alert: Alert, favorites: List[FavoriteResponse]) -> bool:
        if not alert.transport_type: 
            return False

        for fav in favorites:
            if fav.type != alert.transport_type.value: 
                continue

            for entity in alert.affected_entities:
                if entity.station_code:
                    if str(entity.station_code) == fav.station_code:
                        return True
                
                elif entity.line_code:
                    if fav.line_code and str(entity.line_code) == str(fav.line_code):
                        return True

        return False

    async def scheduler(self):
        logger.info(f"Starting Unified Scheduler loop (Interval: {self.interval}s)")
        while self._running:
            now = datetime.now()
            
            try:
                await self.check_new_alerts()
            except Exception as e:
                logger.error(f"Error in transport alerts loop: {e}")

            if now.hour != self._last_card_check_hour:
                try:
                    await self.check_card_expirations()
                    self._last_card_check_hour = now.hour
                except Exception as e:
                    logger.error(f"Error in card expiration loop: {e}")

            try:
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break