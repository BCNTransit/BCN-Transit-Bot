from src.domain.schemas.update import AppVersionResponse, UpdateAction
from src.infrastructure.database.repositories.app_version_repository import AppVersionRepository
from src.infrastructure.database.database import async_session_factory


class AppVersionService:
    def __init__(self):
        self.repository = AppVersionRepository(async_session_factory)

    async def check_update_status(self, platform: str, version_code: int) -> AppVersionResponse:
        config = await self.repository.get_by_platform(platform)

        if not config:
            return AppVersionResponse(
                action=UpdateAction.NONE,
                title_key="",
                message_key="",
                store_url=""
            )

        if version_code < config.min_supported_version_code:
            return AppVersionResponse(
                action=UpdateAction.FORCE,
                title_key=config.force_title_key,
                message_key=config.force_message_key,
                store_url=config.store_url
            )

        if version_code < config.latest_version_code:
            return AppVersionResponse(
                action=UpdateAction.RECOMMEND,
                title_key=config.recommend_title_key,
                message_key=config.recommend_message_key,
                store_url=config.store_url
            )

        # Version code up to date.
        return AppVersionResponse(
            action=UpdateAction.NONE,
            title_key="",
            message_key="",
            store_url=""
        )