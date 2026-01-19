
from src.domain.models.common.station import Station
from src.domain.enums.transport_type import TransportType

class StationMapper:
    
    @staticmethod
    def map_metro_station(feature: dict) -> Station:
        props = feature['properties']
        coords = feature['geometry']['coordinates']

        return Station(
            station_group_code=props.get('CODI_GRUP_ESTACIO',''),
            id=str(props.get('ID_ESTACIO', '')),
            code=str(props.get('CODI_ESTACIO','')),
            name=props.get('NOM_ESTACIO', ''),
            order=props.get('ORDRE_ESTACIO', ''),
            line_code=str(props.get('CODI_LINIA', '')),
            line_name=str(props.get('NOM_LINIA', '')),
            description=props.get('DESC_SERVEI', ''),
            latitude=coords[1],
            longitude=coords[0],
            transport_type=TransportType.METRO
        )
    
    @staticmethod
    def map_bus_stop(feature: dict) -> Station:
        props = feature["properties"]
        coords = tuple(feature["geometry"]["coordinates"])

        return Station(
            id=str(props.get("CODI_PARADA", "")) + "-" + str(props.get("ID_SENTIT", "")),
            code=str(props.get("CODI_PARADA", "")),
            name=props.get("NOM_PARADA", ""),
            description=props.get("DESC_PARADA", ""),
            order=props.get("ORDRE", ""),
            line_code=str(props.get('CODI_LINIA', '')),
            line_name=str(props.get('NOM_LINIA', '')),
            direction=props.get("DESTI_SENTIT", ""),
            latitude=coords[1],
            longitude=coords[0],
            transport_type=TransportType.BUS
        )
    
    @staticmethod
    def map_rodalies_station(station_data, line_code, line_name, order: int = 0) -> Station:
        return Station(
            id=str(station_data["id"]),
            code=str(station_data["id"]),
            order=order,
            name=station_data["name"],
            line_code=line_code,
            line_name=line_name,
            description=None,
            latitude=station_data["latitude"],
            longitude=station_data["longitude"],
            transport_type=TransportType.RODALIES
        )
    
    @staticmethod
    def map_tram_station(props: dict, line_code):
        return Station(
            id=str(props.get('id', '')),
            code=str(props.get('gtfsCode','')),
            name=props.get('name', ''),
            order=props.get('order', ''),
            outboundCode=str(props.get('outboundCode', '')),
            returnCode=str(props.get('returnCode', '')),
            description=props.get('description', ''),
            latitude=props.get('latitude', ''),
            longitude=props.get('longitude', ''),
            transport_type=TransportType.TRAM,
            line_code=str(line_code),
            line_name=str(line_code)
        )
    
    @staticmethod
    def map_fgc_station(station_data, line_code, line_name, order):
        return Station(
            id=str(station_data["stop_id"]),
            code=str(station_data["stop_id"]),
            name=station_data["stop_name"],
            latitude=float(station_data["stop_lat"]),
            longitude=float(station_data["stop_lon"]),
            line_code=line_code,
            line_name=line_name,
            moute_id=str(station_data.get("moute_id")),
            order=order,
            transport_type=TransportType.FGC
        )