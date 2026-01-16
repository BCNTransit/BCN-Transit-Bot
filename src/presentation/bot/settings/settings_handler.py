from telegram import Update
from telegram.ext import ContextTypes

from src.presentation.bot import KeyboardFactory
from src.application.services import audit_action
from src.infrastructure.localization.language_manager import LanguageManager

class SettingsHandler:

    def __init__(self, message_service, keyboard_factory: KeyboardFactory, language_manager: LanguageManager):
        self.message_service = message_service
        self.keyboard_factory = keyboard_factory
        self.language_manager = language_manager

    @audit_action(action_type="SHOW_SETTINGS")
    async def show_settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.message_service.handle_interaction(update, self.language_manager.t('settings.message'), reply_markup=self.keyboard_factory.settings_replykeyboard())
