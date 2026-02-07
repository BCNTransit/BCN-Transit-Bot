

from typing import List, Optional
from pydantic import BaseModel


class FavoriteResponse(BaseModel):
    type: str
    physical_station_id: str
    station_code: str
    station_name: str
    
    line_id: str
    line_name: str
    line_code: str
    
    coordinates: List[float]  # [lat, lon]
    alias: Optional[str] = None

    class Config:
        from_attributes = True

class FavoriteDeleteRequest(BaseModel):
    physical_station_id: str
    line_id: str
    type: str