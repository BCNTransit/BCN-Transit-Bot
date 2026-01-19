from __future__ import annotations
from dataclasses import dataclass

from src.domain.enums.transport_type import TransportType
from src.domain.models.common.line import Line
from src.domain.models.common.station import Station

@dataclass
class MetroStation(Station):
    CODI_GRUP_ESTACIO: int
    ORIGEN_SERVEI: str
    DESTI_SERVEI: str

    @staticmethod
    def update_metro_station_with_line_info(metro_station: Station, metro_line: Line) -> Station:
        metro_station.line_description = metro_line.description
        metro_station.line_id = metro_line.id
        metro_station.line_code = metro_line.code
        metro_station.line_name = metro_line.name
        metro_station.line_name_with_emoji = _set_emoji_at_name(metro_station.line_name)
        if metro_line.has_alerts:
            for alert in metro_line.alerts:
                for entity in alert.affected_entities:
                    if entity.station_code == str(metro_station.code):
                        metro_station.has_alerts = True
                        metro_station.alerts = alert.publications

        return metro_station

def _set_emoji_at_name(name):
    emojis = {
        "L1": "🟥",
        "L2": "🟪",
        "L3": "🟩",
        "L4": "🟨",
        "L5": "🟦",
        "L9N": "🟧",
        "L9S": "🟧",
        "L10N": "🟦",
        "L10S": "🟦",
        "L11": "🟩",
    }
    return f"{emojis.get(name, "")} {name}"

