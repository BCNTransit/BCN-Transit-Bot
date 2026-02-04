from dataclasses import dataclass, field
from typing import Tuple, Optional, List

@dataclass
class NearbyStation:
    type: str
    station_name: str
    station_code: str
    coordinates: Tuple[float, float]
    distance_km: float
    
    # Campos comunes / transporte
    line_name: Optional[str] = ""
    line_code: Optional[str] = ""
    line_name_with_emoji: str = ""
    
    # Campos específicos de Bicing (opcionales)
    slots: Optional[int] = None
    mechanical: Optional[int] = None
    electrical: Optional[int] = None
    availability: Optional[int] = None