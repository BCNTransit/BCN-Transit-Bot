from __future__ import annotations
from dataclasses import dataclass

from src.domain.enums.transport_type import TransportType
from src.domain.models.common.station import Station
from src.domain.models.common.line import Line

@dataclass
class BusStop(Station):
    DESTI_SENTIT: str

    @staticmethod
    def update_bus_stop_with_line_info(bus_stop: Station, bus_line: Line):
        if bus_line.has_alerts:
            for alert in bus_line.alerts:                
                for entity in alert.affected_entities:
                    if entity.line_name == bus_stop.line_name and entity.station_code == bus_stop.code:
                        bus_stop.has_alerts = True
                        if alert.publications not in bus_stop.alerts:
                            bus_stop.alerts = alert.publications

        return bus_stop


