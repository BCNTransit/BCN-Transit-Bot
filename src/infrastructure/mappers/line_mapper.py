from typing import Any, Dict, List
from src.domain.models.common.line import Line
from src.domain.enums.transport_type import TransportType
from src.domain.models.rodalies.rodalies_station import RodaliesStation

class LineMapper:

    @staticmethod
    def resolve_color(name: str, transport_type: TransportType, api_color: str = None) -> str:
        RODALIES_COLORS = {
            "R1": "73B0DF", "R2": "009640", "R2 Nord": "AACB2B", "R2 Sud": "005F27",
            "R3": "E63027", "R4": "F6A22D", "R7": "BC79B2", "R8": "870064",
            "R11": "0064A7", "R13": "E8308A", "R14": "5E4295", "R15": "9A8B75",
            "R16": "B20933", "R17": "E87200", "RG1": "0071CE", "RT1": "00C4B3",
            "RT2": "E577CB", "RL3": "949300", "RL4": "FFDD00",
        }
            
        if api_color and api_color not in ["", None, "null"]:
            return api_color.replace("#", "")

        if transport_type == TransportType.RODALIES:
            return RODALIES_COLORS.get(name, "808080")
        
        # Defaults
        if transport_type == TransportType.METRO: return "D9303D"
        if transport_type == TransportType.BUS: return "D9303D"
        if transport_type == TransportType.FGC: return "F7931D"
        if transport_type == TransportType.TRAM: return "009640"
        
        return "808080"

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
    def map_fgc_connection(id, code, name, description, color):
        return Line(
            id = id,
            code = code,
            name = name,
            description = description,
            origin = '',
            destination = '',
            color = color,
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
        )
    
    @staticmethod
    def map_rodalies_connection(id, code, name, description: str, color) -> Line:
        return Line(
            id=id,
            code=code,
            name=name,
            description=description if description is not None else name,
            transport_type=TransportType.RODALIES,
            origin=description.split("-")[0].strip() if description and "-" in description else '',
            destination=description.split("-")[1].strip() if description and "-" in description else '',
            color=color,
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
    def map_tram_connection(id, code, name, description, origin, destination):
        return Line(
            id=id,
            code=code,
            name=name,
            description=description,
            origin=origin,
            destination=destination,
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