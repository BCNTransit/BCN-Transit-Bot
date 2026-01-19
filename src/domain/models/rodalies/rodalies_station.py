from dataclasses import dataclass
from src.domain.models.common.line import Line
from src.domain.models.common.station import Station
from src.domain.enums.transport_type import TransportType

@dataclass
class RodaliesStation(Station):
    
    @staticmethod
    def update_line_info(rodalies_station: Station, line: Line):
        rodalies_station.line_description = line.description
        rodalies_station.line_name_with_emoji = line.name_with_emoji
        rodalies_station.line_color = line.color
        rodalies_station.line_id = line.id
        rodalies_station.line_name = line.name
        rodalies_station.line_code = line.id