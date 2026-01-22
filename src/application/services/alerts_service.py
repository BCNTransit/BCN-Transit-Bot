from __future__ import annotations
from typing import TYPE_CHECKING, List

import asyncio
import html
import os

from telegram import Bot
from telegram.error import TelegramError
from firebase_admin import messaging

from src.domain.enums.clients import ClientType
from src.domain.models.common.alert import Alert
from src.domain.models.common.user import User
from src.domain.schemas.favorite import FavoriteResponse
from src.application.services.user_data_manager import UserDataManager
from src.core.logger import logger

if TYPE_CHECKING:
    from src.application.services.message_service import MessageService

class AlertsService:
    def __init__(self, bot: Bot, message_service: MessageService, user_data_manager: UserDataManager, interval: int = 300):
        self.bot = bot
        self.message_service = message_service
        self.user_data_manager = user_data_manager
        
        env_interval = os.getenv("ALERTS_SERVICE_INTERVAL")
        self.interval = int(env_interval) if env_interval else interval
        
        self._running = False
        self._task = None
        self._semaphore = asyncio.Semaphore(10) 

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
        """Envía push notification usando el Executor para no bloquear."""
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

    async def _notify_user(self, user: User, alert: Alert):
        """
        Notifica a un usuario específico (Android o Telegram).
        Usa semáforo para controlar la carga.
        """
        async with self._semaphore:
            already_sent = await self.user_data_manager.has_notification_been_sent(user.user_id, alert.id)
            if already_sent:
                return

            notification_sent = False

            try:
                # CASO A: ANDROID (Tiene token FCM)
                if user.fcm_token:
                    logger.info(f"🔔 Sending PUSH to {user.user_id[:8]}... (Alert {alert.id})")
                    await self.send_push_notification(
                        user.fcm_token,
                        title="BCN Transit | Incidencia",
                        body=Alert.format_app_alert(alert),
                        data={
                            "alert_id": str(alert.id), 
                            "click_action": "FLUTTER_NOTIFICATION_CLICK",
                            "type": "incident"
                        }
                    )
                    notification_sent = True

                # CASO B: TELEGRAM (Tiene auth_provider telegram o user_id numérico)
                elif user.auth_provider == "telegram" or (user.user_id.isdigit() and len(user.user_id) < 15):
                    logger.info(f"✈️ Sending TELEGRAM to {user.user_id} (Alert {alert.id})")
                    await self.message_service.send_new_message_from_bot(
                        self.bot, 
                        user.user_id, 
                        Alert.format_html_alert(alert)
                    )
                    notification_sent = True

                if notification_sent:
                    await self.user_data_manager.log_notification_sent(
                        user_id=user.user_id,
                        alert_id=alert.id,
                        client_source=ClientType.ANDROID if user.fcm_token else ClientType.TELEGRAM
                    )

            except TelegramError as te:
                if "Forbidden" in str(te):
                    logger.warning(f"User {user.user_id} blocked the bot. Skipping.")
                else:
                    logger.error(f"Telegram error for {user.user_id}: {te}")
            except Exception as e:
                logger.error(f"Failed to notify user {user.user_id}: {e}")

    def _is_alert_relevant_for_user(self, alert: Alert, favorites: List[FavoriteResponse]) -> bool:
        """
        Determina si una alerta afecta a alguno de los favoritos del usuario.
        """
        if not alert.transport_type: return False

        for fav in favorites:
            if fav.type != alert.transport_type.value:
                continue

            for entity in alert.affected_entities:
                if entity.station_code and str(entity.station_code) == fav.station_code:
                    return True
                
                if entity.line_code and fav.line_code and str(entity.line_code) == str(fav.line_code):
                    return True

        return False

    async def check_new_alerts(self):
        try:
            alerts = await self.user_data_manager.get_alerts(only_active=True)
            if not alerts: return

            users_data = await self.user_data_manager.get_active_users_with_favorites()
            
            if not users_data: return

            logger.info(f"🔎 Checking {len(alerts)} alerts for {len(users_data)} active users...")

            tasks = []

            for user, favorites in users_data:
                
                if not user.receive_notifications:
                    continue

                for alert in alerts:
                    if not self._is_alert_relevant_for_user(alert, favorites):
                        continue

                    is_already_notified = await self.user_data_manager.has_notification_been_sent(user.user_id, alert.id)
                    
                    if not is_already_notified:
                        tasks.append(self._notify_user(user, alert))

            if tasks:
                logger.info(f"📨 Dispatching {len(tasks)} notifications...")
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.exception(f"❌ Critical error checking alerts: {e}")

    async def scheduler(self):
        logger.info(f"Starting Alert Scheduler loop (Interval: {self.interval}s)")
        while self._running:
            try:
                await self.check_new_alerts()
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
            
            try:
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break
        logger.info("Scheduler loop exited.")