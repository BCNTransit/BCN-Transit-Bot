import aiohttp
import inspect
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, List

from src.domain.models.common.station import Station
from src.infrastructure.mappers.station_mapper import StationMapper
from src.domain.enums.transport_type import TransportType
from src.core.logger import logger

from src.domain.models.common.next_trip import NextTrip, normalize_to_seconds
from src.domain.models.common.line import Line
from src.domain.models.common.line_route import LineRoute
from src.infrastructure.mappers.line_mapper import LineMapper


class RodaliesApiService:
    """Service to interact with Rodalies de Catalunya API."""

    BASE_URL = "https://serveisgrs.rodalies.gencat.cat/api"

    def __init__(self):
        self.logger = logger.getChild(self.__class__.__name__)

    async def _request(self, method: str, endpoint: str, use_base_url: bool = True, **kwargs) -> Any:
        """Generic HTTP request handler with token authentication."""
        current_method = inspect.currentframe().f_code.co_name
        headers = kwargs.pop("headers", {})
        headers["Accept"] = "application/json"

        url = f"{self.BASE_URL}{endpoint}" if use_base_url else endpoint
        self.logger.debug(f"[{current_method}] {method.upper()} → {url} | Params: {kwargs.get('params', {})}")

        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, headers=headers, **kwargs) as resp:
                if resp.status == 401:
                    self.logger.warning(f"[{current_method}] Token expired → retrying")
                    async with session.request(method, url, headers=headers, **kwargs) as retry_resp:
                        retry_resp.raise_for_status()
                        return await retry_resp.json()

                resp.raise_for_status()
                return await resp.json()

    # ==== Lines ====
    async def get_lines(self, type: str = "RODALIES") -> List[Line]:
        """Fetch all Rodalies lines."""

        lines = []
        for type in ["RODALIES", "REGIONAL"]:
            data = await self._request("GET", f"/lines?type={type}&page=0&limit=100&lang=ca", params=None)

            lines.extend(
                LineMapper.map_rodalies_line(line_data)
                for line_data in data["included"]
            )
        return lines

    async def get_line_by_id(self, line_id: int) -> Line:
        """Fetch a single Rodalies line by ID."""
        line_data = await self._request("GET", f"/lines/{line_id}")
        return LineMapper.map_rodalies_line(line_data)
    
    async def get_stations_by_line_id(self, line_id: int) -> List[Station]:
        """Fetch a single Rodalies line by ID."""
        line_data = await self._request("GET", f"/lines/{line_id}")
        stations = []
        stations.extend(
            StationMapper.map_rodalies_station(station_data, line_code=line_data.get('id'), line_name=line_data.get('name'), order=i)
            for i, station_data in enumerate(line_data["stations"], start = 1)
        )
        return stations

    async def get_global_alerts(self):
        alerts = await self._request("GET", "/notices?limit=500&sort=date,desc&sort=time,desc")
        return alerts['included']

    # ==== Stations ====
    async def get_next_trains_at_station(self, station_id: int) -> List[LineRoute]:
        next_rodalies = await self._request("GET", f"/departures?stationId={station_id}&minute=90&fullResponse=true&lang=ca")        
        
        madrid_tz = ZoneInfo("Europe/Madrid")
        
        if "trains" not in next_rodalies:
            return []
        
        routes_dict = {}
        
        for item in next_rodalies["trains"]:
            line = item["line"]
            api_line_name = line.get("name", "").upper()
            if not api_line_name:
                continue

            key = (api_line_name, line["id"], item["destinationStation"]["name"])

            dt_naive = datetime.fromisoformat(item["departureDateHourSelectedStation"])
            dt_aware = dt_naive.replace(tzinfo=madrid_tz)
            utc_timestamp = dt_aware.timestamp()

            if utc_timestamp < datetime.now(tz=madrid_tz).timestamp():
                continue
            
            next_trip = NextTrip(
                    id=item["technicalNumber"],
                    arrival_time=normalize_to_seconds(utc_timestamp), 
                    platform=item["platformSelectedStation"],
                    delay_in_minutes=item["delay"]
                )
            
            if key not in routes_dict:
                routes_dict[key] = LineRoute(
                    route_id=item["commercialNumber"],
                    line_name=api_line_name,
                    line_id=api_line_name, 
                    line_code=line["id"],
                    destination=item["destinationStation"]["name"],
                    next_trips=[next_trip],
                    color=None,
                    line_type=TransportType.RODALIES
                )
            else:
                routes_dict[key].next_trips.append(next_trip)

        return list(routes_dict.values())
        