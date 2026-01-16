from telegram import Update
from telegram.ext import ContextTypes

from src.presentation.bot.keyboard_factory import KeyboardFactory
from src.application.services import UserDataManager, audit_action
from src.infrastructure.localization.language_manager import LanguageManager

class HelpHandler:

    def __init__(self, message_service, keyboard_factory: KeyboardFactory, language_manager: LanguageManager, user_data_manager: UserDataManager):
        self.message_service = message_service
        self.keyboard_factory = keyboard_factory
        self.language_manager = language_manager
    
    @audit_action(action_type="SHOW_HELP")
    async def show_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.message_service.handle_interaction(update, self.language_manager.t('help.text'), reply_markup=self.keyboard_factory._back_reply_button())
