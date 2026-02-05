from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

@dataclass
class NearbyStation:
    type: str
    station_name: str
    physical_station_id: str
    coordinates: Tuple[float, float]
    distance_km: float
    
    lines: List[Dict[str, str]] = field(default_factory=list)
    
    # Campos específicos de Bicing (opcionales)
    slots: Optional[int] = None
    mechanical: Optional[int] = None
    electrical: Optional[int] = None
    availability: Optional[int] = None