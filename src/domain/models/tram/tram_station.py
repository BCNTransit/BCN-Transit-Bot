from dataclasses import dataclass

from src.domain.enums.transport_type import TransportType
from src.domain.models.common.station import Station
from src.domain.models.common.line import Line

@dataclass
class TramStation(Station):
    outboundCode: int
    returnCode: int
    
    @staticmethod
    def update_line_info(tram_station: Station, line: Line):
        tram_station.line_description = line.description
        tram_station.line_name_with_emoji = line.name_with_emoji
        tram_station.line_name = line.name
        tram_station.line_code = line.code
        tram_station.line_id = line.id
        tram_station.line_color = line.color
        return tram_station