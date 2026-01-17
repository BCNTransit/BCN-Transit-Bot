from src.domain.schemas.favorite import FavoriteResponse 
from src.application.utils.bool_converter import BoolConverter
from telegram import Update
from telegram.ext import ContextTypes

from src.application.services.transport.metro_service import MetroService
from src.application.services.transport.bus_service import BusService
from src.application.services.transport.tram_service import TramService
from src.application.services.transport.rodalies_service import RodaliesService
from src.application.services.transport.bicing_service import BicingService
from src.application.services.transport.fgc_service import FgcService
from src.presentation.bot.keyboard_factory import KeyboardFactory

from src.domain.enums.clients import ClientType
from src.domain.enums.transport_type import TransportType
from src.infrastructure.localization.language_manager import LanguageManager
from src.application.services.user_data_manager import UserDataManager
from src.application.services.message_service import MessageService

class FavoritesHandler:

    def __init__(self, message_service: MessageService, user_data_manager: UserDataManager, keyboard_factory: KeyboardFactory, metro_service: MetroService, bus_service: BusService, tram_service: TramService, rodalies_service: RodaliesService, bicing_service: BicingService, fgc_service: FgcService, language_manager: LanguageManager):
        self.message_service = message_service
        self.user_data_manager = user_data_manager
        self.keyboard_factory = keyboard_factory
        self.metro_service = metro_service
        self.bus_service = bus_service
        self.tram_service = tram_service
        self.rodalies_service = rodalies_service
        self.bicing_service = bicing_service
        self.fgc_service = fgc_service
        self.language_manager = language_manager      

    async def show_favorites(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = self.message_service.get_user_id(update)
        favs = await self.user_data_manager.get_favorites_by_user(ClientType.TELEGRAM.value, user_id)

        await self.message_service.send_new_message(
            update,
            self.language_manager.t('common.loading.favorites'),
            reply_markup=self.keyboard_factory._back_reply_button()
        )

        if favs == []:
            await self.message_service.send_new_message(
                update,
                self.language_manager.t('favorites.empty'),
                reply_markup=self.keyboard_factory.help_menu()
            )

        else:
            await self.message_service.send_new_message(
                update,
                self.language_manager.t('favorites.message'),
                reply_markup=self.keyboard_factory.favorites_menu(favs)
            )

    async def add_favorite(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        user_id = query.from_user.id
        data = query.data
        _, item_type, line_id, item_id, previous_callback, has_connections = data.split(":")

        # Añadir favorito
        if item_type == TransportType.METRO.value:
            item = await self.metro_service.get_station_by_code(item_id)
            
            new_fav_item = FavoriteResponse(
                type=item_type,
                station_code=str(item.code),
                station_name=item.name,
                station_group_code=str(item.CODI_GRUP_ESTACIO),
                line_name=item.line_name,
                line_name_with_emoji=item.line_name_with_emoji,
                line_code=line_id,
                coordinates=[item.latitude, item.longitude]
            )
        elif item_type == TransportType.BUS.value:
            item = await self.bus_service.get_stop_by_code(item_id)

            new_fav_item = FavoriteResponse(
                type=item_type,
                station_code=str(item.code),
                station_name=item.name,
                station_group_code='',
                line_name=item.line_name,
                line_name_with_emoji=item.line_name_with_emoji,
                line_code=line_id,
                coordinates=[item.latitude, item.longitude]
            )
        elif item_type == TransportType.TRAM.value:
            item = await self.tram_service.get_stop_by_id(item_id)
            line = await self.tram_service.get_line_by_id(line_id)      

            new_fav_item = FavoriteResponse(
                type=item_type,
                station_code=str(item.code),
                station_name=item.name,
                station_group_code='',
                line_name=item.line_name,
                line_name_with_emoji=item.line_name_with_emoji,
                line_code=line_id,
                coordinates=[item.latitude, item.longitude]
            )
        elif item_type == TransportType.RODALIES.value:
            item = await self.rodalies_service.get_station_by_id(item_id)
            line = await self.rodalies_service.get_line_by_id(line_id)

            new_fav_item = FavoriteResponse(
                type=item_type,
                station_code=str(item.code),
                station_name=item.name,
                station_group_code='',
                line_name=item.line_name,
                line_name_with_emoji=item.line_name_with_emoji,
                line_code=line_id,
                coordinates=[item.latitude, item.longitude]
            )
                
        elif item_type == TransportType.BICING.value:
            item = await self.bicing_service.get_station_by_id(item_id)

            new_fav_item = FavoriteResponse(
                type=item_type,
                station_code=item.id,
                station_name=item.streetName,
                station_group_code='',
                line_name='',
                line_name_with_emoji='',
                line_code='',
                coordinates=[item.latitude, item.longitude]
            )
                
        elif item_type == TransportType.FGC.value:
            item = await self.fgc_service.get_station_by_id(item_id, line_id)
            line = await self.fgc_service.get_line_by_id(line_id)

            new_fav_item = FavoriteResponse(
                type=item_type,
                station_code=str(item.code),
                station_name=item.name,
                station_group_code='',
                line_name=line.name,
                line_name_with_emoji=line.name_with_emoji,
                line_code=line_id,
                coordinates=[item.latitude, item.longitude]
            )

        await self.user_data_manager.add_favorite(ClientType.TELEGRAM.value, user_id, item_type, new_fav_item)
        keyboard = self.keyboard_factory.update_menu(is_favorite=True, item_type=item_type, item_id=item_id, line_id=line_id, previous_callback=previous_callback, has_connections=BoolConverter.from_string(has_connections))

        await query.edit_message_reply_markup(reply_markup=keyboard)

    async def remove_favorite(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        user_id = query.from_user.id
        data = query.data
        _, item_type, line_id, item_id, previous_callback, has_connections = data.split(":")

        await self.user_data_manager.remove_favorite(ClientType.TELEGRAM.value, user_id, item_type, item_id)
        keyboard = self.keyboard_factory.update_menu(is_favorite=False, item_type=item_type, item_id=item_id, line_id=line_id, previous_callback=previous_callback, has_connections=BoolConverter.from_string(has_connections))

        await query.edit_message_reply_markup(reply_markup=keyboard)