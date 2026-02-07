from pydantic import BaseModel, Field
from typing import Optional, Tuple

class StationSearchResult(BaseModel):
    physical_station_id: str
    station_external_code: str
    line_id: str
    
    station_name: str
    line_name: str
    line_color: str
    
    type: str
    match_score: float
    
    coordinates: Optional[Tuple[float, float]] = None
    has_alerts: bool = False

    class Config:
        from_attributes = True