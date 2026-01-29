from fastapi import Depends, FastAPI
from src.presentation.api.api import get_metro_router, get_bus_router, get_tram_router, get_rodalies_router, get_bicing_router, get_fgc_router, get_results_router, get_user_router
from src.presentation.api.auth import get_api_key

def create_app(
    metro_service,
    bus_service,
    tram_service,
    rodalies_service,
    bicing_service,
    fgc_service,
    user_data_manager
):
    app = FastAPI(title="BCN Transit API", version="1.0.0")

    app.include_router(get_metro_router(metro_service), prefix="/api/metro", tags=["Metro"], dependencies=[Depends(get_api_key)])
    app.include_router(get_bus_router(bus_service), prefix="/api/bus", tags=["Bus"], dependencies=[Depends(get_api_key)])
    app.include_router(get_tram_router(tram_service), prefix="/api/tram", tags=["Tram"], dependencies=[Depends(get_api_key)])
    app.include_router(get_fgc_router(fgc_service), prefix="/api/fgc", tags=["FGC"], dependencies=[Depends(get_api_key)])
    app.include_router(get_rodalies_router(rodalies_service), prefix="/api/rodalies", tags=["Rodalies"], dependencies=[Depends(get_api_key)])
    app.include_router(get_bicing_router(bicing_service), prefix="/api/bicing", tags=["Bicing"], dependencies=[Depends(get_api_key)])

    app.include_router(get_results_router(metro_service, bus_service, tram_service, rodalies_service, bicing_service, fgc_service, user_data_manager), prefix="/api/results", tags=["Search Stations"], dependencies=[Depends(get_api_key)])

    app.include_router(get_user_router(user_data_manager), prefix="/api/users", tags=["Users"], dependencies=[Depends(get_api_key)])

    return app