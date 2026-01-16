

from typing import List
from pydantic import BaseModel


class FavoriteResponse(BaseModel):
    type: str
    station_code: str
    station_name: str
    station_group_code: str
    line_name: str
    line_name_with_emoji: str
    line_code: str
    coordinates: List[float]

class FavoriteDeleteRequest(BaseModel):
    type: str
    station_code: str