from pydantic import BaseModel, ConfigDict
from typing import Optional

class StationSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    code: str
    order: Optional[int] = 0
    latitude: float
    longitude: float
    transport_type: str
    has_alerts: bool = False
    
    # ⚠️ NO incluyas campos complejos como 'connections_data' 
    # ni objetos que den error de serialización.