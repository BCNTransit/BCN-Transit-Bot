from typing import Any, Dict, List
from src.domain.models.common.line import Line
from src.domain.enums.transport_type import TransportType
from src.domain.models.rodalies.rodalies_station import RodaliesStation

class LineMapper:

    @staticmethod
    def map_metro_line(feature: Dict[str, Any]) -> Line:
        props = feature.get('properties', {})
        return Line(
            id=str(props.get('ID_LINIA', '')),
            code=str(props.get('CODI_LINIA', '')),
            name=props.get('NOM_LINIA', ''),
            description=props.get('DESC_LINIA', ''),
            origin=props.get('ORIGEN_LINIA', ''),
            destination=props.get('DESTI_LINIA', ''),
            color=props.get('COLOR_LINIA', ''),
            transport_type=TransportType.METRO
        )
    
    @staticmethod
    def map_fgc_line(data: Dict[str, Any]) -> Line:
        long_name = data.get('route_long_name', '')
        parts = long_name.split("-") if "-" in long_name else ["", ""]
        
        return Line(
            id=str(data.get('route_id')),
            code=str(data.get('route_id')),
            name=data.get('route_short_name'),
            description=long_name,
            origin=parts[0].strip(),
            destination=parts[1].strip() if len(parts) > 1 else "",
            color=data.get('route_color'),
            transport_type=TransportType.FGC
        )
    
    @staticmethod
    def map_rodalies_line(data: Dict[str, Any], stations: List[RodaliesStation]) -> Line:
        return Line(
            id=str(data["id"]),
            code=str(data["id"]),
            name=data["name"],
            description=data.get("journeyDescription", ""),
            transport_type=TransportType.RODALIES,
            origin=data.get("originStation", {}).get("name", ""),
            destination=data.get("destinationStation", {}).get("name", ""),
            stations=stations,
            color="808080"
        )
    
    @staticmethod
    def map_tram_line(props: Dict[str, Any]) -> Line:
        return Line(
            id=str(props.get('id', '')),
            code=str(props.get('code', '')),
            name=props.get('name', ''),
            description='TBD',
            origin='',
            destination='',
            color="008E78",
            transport_type=TransportType.TRAM
        )
    
    @staticmethod
    def map_bus_line(feature: Dict[str, Any]) -> Line:
        props = feature.get('properties', {})
        
        return Line(
            id=str(props.get('ID_LINIA', '')),
            code=str(props.get('CODI_LINIA', '')),
            name=props.get('NOM_LINIA', ''),
            description=props.get('DESC_LINIA', ''),
            origin=props.get('ORIGEN_LINIA', ''),
            destination=props.get('DESTI_LINIA', ''),
            color=props.get('COLOR_LINIA', ''),
            transport_type=TransportType.BUS,            
            category=props.get('NOM_FAMILIA', '') 
        )