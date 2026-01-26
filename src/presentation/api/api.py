from typing import List, Optional

import asyncio
from fastapi import APIRouter

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.params import Body
from pydantic import BaseModel, Field

from firebase_admin import auth
from src.domain.models.common.user_settings import UserSettingsResponse, UserSettingsUpdate
from src.domain.models.common.card import CardCreate, CardResponse, CardUpdate
from src.domain.schemas.models import DBUser, UserDevice, UserSource
from src.infrastructure.database.repositories.user_repository import UserRepository
from src.presentation.api.auth import get_current_user_uid

from src.application.services.user_data_manager import UserDataManager
from src.application.services.transport.bicing_service import BicingService
from src.application.services.transport.bus_service import BusService
from src.application.services.transport.fgc_service import FgcService
from src.application.services.transport.metro_service import MetroService
from src.application.services.transport.rodalies_service import RodaliesService
from src.application.services.transport.tram_service import TramService

from src.application.utils.distance_helper import DistanceHelper
from src.application.utils.utils import Utils

from src.domain.enums.clients import ClientType

from src.domain.schemas.favorite import FavoriteResponse

from src.domain.models.common.line import Line
from src.domain.models.common.location import Location

from src.infrastructure.database.database import async_session_factory



class RegisterDeviceRequest(BaseModel):
    user_id: str
    fcm_token: str = ""
    username: str = ""

    
class GoogleLoginRequest(BaseModel):
    user_id: str
    id_token: str
    fcm_token: str

class UpdateFavoriteAliasRequest(BaseModel):
    alias: Optional[str] = Field(None, max_length=50, description="El nuevo nombre personalizado para la estación")

def get_metro_router(
    metro_service: MetroService
) -> APIRouter:
    router = APIRouter()

    @router.get("/lines", response_model=List[Line])
    async def list_metro_lines():
        return await metro_service.get_all_lines()
    
    @router.get("/lines/{line_code}/stations")
    async def list_metro_stations_by_line(line_code: str):
        return await metro_service.get_stations_by_line_code(line_code)
    
    @router.get("/stations/{station_code}")
    async def get_metro_station(station_code: str):
        return await metro_service.get_station_by_code(station_code)
    
    @router.get("/stations/{station_code}/routes")
    async def list_metro_station_routes(station_code: str):
        return await metro_service.get_station_routes(station_code)
    
    @router.get("/stations/{station_code}/accesses")
    async def get_metro_station_accesses(station_code: str):
        station = await metro_service.get_station_by_code(station_code)
        if station:
            return await metro_service.get_station_accesses(station.station_group_code)
        else:
            return []

    return router

def get_bus_router(
    bus_service: BusService
) -> APIRouter:
    router = APIRouter()

    @router.get("/lines")
    async def list_bus_lines():
        return await bus_service.get_all_lines()
    
    @router.get("/lines/{line_code}/stops")
    async def list_bus_stations_by_line(line_code: str):
        return await bus_service.get_stops_by_line_code(line_code)
    
    @router.get("/stops/{stop_code}")
    async def get_bus_stop(stop_code: str):
        return await bus_service.get_stop_by_code(stop_code)
    
    @router.get("/stops/{stop_code}/routes")
    async def list_bus_stop_routes(stop_code: str):
        return await bus_service.get_stop_routes(stop_code)
    
    @router.get("/stops/{stop_code}/accesses")
    async def get_bus_stop_accesses(stop_code: str):
        return []

    return router

def get_tram_router(
    tram_service: TramService
) -> APIRouter:
    router = APIRouter()

    @router.get("/lines")
    async def list_tram_lines():
        return sorted(await tram_service.get_all_lines(), key=Utils.sort_lines)
    
    @router.get("/lines/{line_code}/stops")
    async def list_tram_stops_by_line(line_code: str):
        return await tram_service.get_stations_by_line_code(line_code)    
    
    @router.get("/stops/{stop_code}")
    async def get_tram_stop(stop_code: str):
        return await tram_service.get_stop_by_code(stop_code)
    
    @router.get("/stops/{stop_code}/routes")
    async def list_tram_stop_routes(stop_code: str):
        return await tram_service.get_stop_routes(stop_code)
    
    @router.get("/stops/{stop_code}/accesses")
    async def get_tram_stop_accesses(stop_code: str):
        return []

    return router

def get_rodalies_router(
    rodalies_service: RodaliesService
) -> APIRouter:
    router = APIRouter()

    @router.get("/lines")
    async def list_rodalies_lines():
        return sorted(await rodalies_service.get_all_lines(), key=Utils.sort_lines)
    
    @router.get("/lines/{line_code}/stations")
    async def list_rodalies_stations_by_line(line_code: str):
        return await rodalies_service.get_stations_by_line_code(line_code)
    
    @router.get("/stations/{station_code}")
    async def get_rodalies_station(station_code: str):
        return await rodalies_service.get_station_by_code(station_code)
    
    @router.get("/stations/{station_code}/routes")
    async def list_rodalies_station_routes(station_code: str):
        return await rodalies_service.get_station_routes(station_code)

    @router.get("/stations/{station_code}/accesses")
    async def get_rodalies_station_accesses(station_code: str):
        return []

    return router

def get_bicing_router(
    bicing_service: BicingService
) -> APIRouter:
    router = APIRouter()

    @router.get("/stations")
    async def list_bicing_stations():
        return await bicing_service.get_all_stations()
    
    @router.get("/stations/{station_id}")
    async def get_bicing_station(station_id: str):
        return await bicing_service.get_station_by_id(station_id)

    return router

def get_fgc_router(
    fgc_service: FgcService
) -> APIRouter:
    router = APIRouter()

    @router.get("/lines")
    async def list_fgc_lines():
        return sorted(await fgc_service.get_all_lines(), key=Utils.sort_lines)
    
    @router.get("/lines/{line_code}/stations")
    async def list_fgc_stations_by_line(line_code: str):
        return await fgc_service.get_stations_by_line_code(line_code)    
    
    @router.get("/stations/{station_code}")
    async def get_fgc_station(station_code: str):
        return await fgc_service.get_station_by_code(station_code)
    
    @router.get("/stations/{station_code}/routes")
    async def list_fgc_station_routes(station_code: str):
        return await fgc_service.get_station_routes(station_code)

    @router.get("/stations/{station_code}/accesses")
    async def get_fgc_station_accesses(station_code: str):
        return []

    return router



def get_results_router(
    metro_service: MetroService,
    bus_service: BusService,
    tram_service: TramService,
    rodalies_service: RodaliesService,
    bicing_service: BicingService,
    fgc_service: FgcService,
    user_data_manager: UserDataManager
) -> APIRouter:
    router = APIRouter()

    @router.get("/near")
    async def list_near_stations(lat: float, lon: float, radius: float = 0.5):
        metro_task = metro_service.get_stations_by_name('')
        bus_task = bus_service.get_stops_by_name('')
        tram_task = tram_service.get_stations_by_name('')
        fgc_task = fgc_service.get_stations_by_name('')
        rodalies_task = rodalies_service.get_stations_by_name('')
        bicing_task = bicing_service.get_stations_by_name('')

        metro, bus, tram, fgc, rodalies, bicing = await asyncio.gather(
            metro_task, bus_task, tram_task, fgc_task, rodalies_task, bicing_task
        )

        near_results = DistanceHelper.build_stops_list(
            stations=metro + bus + tram + fgc + rodalies,
            bicing_stations=bicing,
            user_location=Location(latitude=lat, longitude=lon),
            results_to_return=999999,
            max_distance_km=radius
        )
        return near_results

    @router.get("/search")
    async def search_stations(name: str, uid: str = Depends(get_current_user_uid)):
        if uid:
             asyncio.create_task(
                 user_data_manager.register_search(
                     query=name,
                     client_source=ClientType.ANDROID,
                     user_id=uid
                 )
             )

        tasks = [
            metro_service.get_stations_by_name(name),
            bus_service.get_stops_by_name(name),
            tram_service.get_stations_by_name(name),
            fgc_service.get_stations_by_name(name),
            rodalies_service.get_stations_by_name(name),
            bicing_service.get_stations_by_name(name),
        ]
        metro, bus, tram, fgc, rodalies, bicing = await asyncio.gather(*tasks)

        search_results = DistanceHelper.build_stops_list(
            stations=metro + bus + tram + fgc + rodalies,
            bicing_stations=bicing
        )
        return search_results

    @router.get("/search/history")
    async def search_history(uid: str = Depends(get_current_user_uid)):
        if uid:
            return await user_data_manager.get_search_history(client_source=ClientType.ANDROID, user_id=uid)
        return []

    return router

def get_user_router(
    user_data_manager: UserDataManager
) -> APIRouter:
    router = APIRouter()

    @router.post("/register-device", status_code=status.HTTP_201_CREATED)
    async def register_device(
        request: RegisterDeviceRequest = Body(...)
    ):
        try:
            is_new = await user_data_manager.register_user(
                client_source=ClientType.ANDROID,
                user_id=request.user_id,
                username=request.username,
                fcm_token=request.fcm_token
            )
            return {"status": "ok", "is_new_user": is_new}
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error registering user: {str(e)}")
        
    @router.post("/auth/google")
    async def google_login(
        request: GoogleLoginRequest,
    ):
        try:
            decoded_token = auth.verify_id_token(request.id_token)
            email = decoded_token.get('email')
            uid = decoded_token.get('uid')
            photo_url = decoded_token.get('picture')
            name = decoded_token.get('name')
            
            if not email:
                raise HTTPException(status_code=400, detail="El token no contiene email")

            user_repo = UserRepository(async_session_factory)

            user = await user_repo.get_by_email(email)
            if user:
                if user.username != name: 
                    user.username = name
                if user.photo_url != photo_url:
                    user.photo_url = photo_url

                device_exists = any(d.installation_id == request.user_id for d in user.devices)

                if not device_exists:
                    new_device = UserDevice(
                        installation_id=request.user_id,
                        fcm_token=request.fcm_token
                    )
                    await user_repo.add_device_to_user(user.id, new_device)

                return {"status": "success", "user_id": user.id}
            
            user_to_migrate = await user_repo.get_user_by_installation_id(request.user_id)
            if user_to_migrate:                
                user_to_migrate.email = email
                user_to_migrate.firebase_uid = uid
                user_to_migrate.photo_url = photo_url
                user_to_migrate.username = name
                user_to_migrate.source = UserSource.ANDROID
                
                await user_repo.update(user_to_migrate)
                
                return {"status": "merged", "message": "Cuenta recuperada y vinculada"}
            
            new_user = DBUser(
                email=email,
                firebase_uid=uid,
                photo_url=photo_url,
                username=name,
                source=UserSource.ANDROID
            )
            
            new_device = UserDevice(
                installation_id=request.user_id,
                fcm_token=request.fcm_token
            )

            await user_repo.create_with_device(new_user, new_device)
            
            return {"status": "created"}

        except auth.InvalidIdTokenError as e:
            raise HTTPException(status_code=401, detail=f"Token inválido: {e}")
        except Exception as e:
            print(f"Error en login: {e}") 
            raise HTTPException(status_code=500, detail="Error interno del servidor")

    @router.patch("/settings", response_model=bool)
    async def update_settings(
        settings_update: UserSettingsUpdate,
        uid: int = Depends(get_current_user_uid)
    ):
        return await user_data_manager.update_user_settings(ClientType.ANDROID, uid, settings_update)
    
    @router.get("/settings", response_model=UserSettingsResponse)
    async def get_settings(
        user_id: int = Depends(get_current_user_uid)
    ):
        return await user_data_manager.get_user_settings(ClientType.ANDROID, user_id)

    @router.get("/favorites/exists")
    async def has_favorite(
        uid: str = Depends(get_current_user_uid),
        type: str = Query(..., description="Tipo de favorito, ej: metro, bus"),
        item_id: str = Query(..., description="Código del item a buscar")
    ) -> bool:
        try:
            exists = await user_data_manager.check_favorite_exists(
                client_source=ClientType.ANDROID, 
                user_id=uid, 
                transport_type=type, 
                item_id=item_id
            )
            return exists
        except Exception as e:
            raise HTTPException(status_code=500, detail="Error checking favorite status")
    
    @router.get("/favorites", response_model=List[FavoriteResponse])
    async def get_favorites(uid: str = Depends(get_current_user_uid)):
        try:
            return await user_data_manager.get_favorites_by_user(ClientType.ANDROID.value, uid)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        
    @router.post("/favorites", status_code=status.HTTP_201_CREATED)
    async def add_favorite(uid: str = Depends(get_current_user_uid), body: FavoriteResponse = Body(...)) -> bool:
        try:
            return await user_data_manager.add_favorite(ClientType.ANDROID.value, uid, type=body.type, item=body)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.patch("/favorites/{transport_type}/{station_code}/alias")
    async def update_favorite_alias(
        transport_type: str,
        station_code: str,
        body: UpdateFavoriteAliasRequest,
        uid: str = Depends(get_current_user_uid)
    ):
        try:
            success = await user_data_manager.update_favorite_alias(
                client_source=ClientType.ANDROID,
                user_id=uid,
                transport_type=transport_type,
                station_code=station_code,
                new_alias=body.alias
            )

            if not success:
                raise HTTPException(status_code=404, detail="Favorite not found")
            
            return {"status": "success", "alias": body.alias}

        except Exception as e:
            raise HTTPException(status_code=500, detail="Internal Server Error")
    
    @router.delete("/favorites")
    async def delete_favorite(
        uid: str = Depends(get_current_user_uid),
        type: str = Query(..., description="Tipo de favorito, ej: metro, bus"),
        item_id: str = Query(..., description="Código del item a eliminar")
    ) -> bool:
        try:
            return await user_data_manager.remove_favorite(ClientType.ANDROID.value, uid, type=type, item_id=item_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/cards", response_model=List[CardResponse])
    async def get_my_cards(
        uid: int = Depends(get_current_user_uid)
    ):
        return await user_data_manager.get_user_cards(ClientType.ANDROID.value, uid)

    @router.post("/cards", response_model=bool, status_code=status.HTTP_201_CREATED)
    async def create_card(
        card_data: CardCreate,
        uid: int = Depends(get_current_user_uid)
    ):
        return await user_data_manager.create_user_card(ClientType.ANDROID.value, uid, card_data)
    
    @router.put("/cards", response_model=bool)
    async def update_card(
        card_data: CardUpdate,
        uid: int = Depends(get_current_user_uid)
    ):
        return await user_data_manager.update_user_card(ClientType.ANDROID.value, uid, card_data)

    @router.delete("/cards/{card_id}", response_model=bool)
    async def delete_card(
        card_id: int,
        uid: int = Depends(get_current_user_uid)
    ):
        return await user_data_manager.remove_user_card(ClientType.ANDROID.value, uid, card_id)

    return router
