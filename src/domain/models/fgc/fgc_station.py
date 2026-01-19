from dataclasses import dataclass
from typing import Optional
from src.domain.models.common.line import Line
from src.domain.models.common.station import Station
from src.domain.enums.transport_type import TransportType

@dataclass
class FgcStation(Station):
    moute_id: Optional[int] =  None
    
    @staticmethod
    def update_line_info(fgc_station: Station, line: Line):
        fgc_station.line_description = line.description
        fgc_station.line_name_with_emoji = line.name_with_emoji
        fgc_station.line_color = line.color
        fgc_station.line_id = line.id
        fgc_station.line_name = line.name
        fgc_station.line_code = line.id
        
        return fgc_station