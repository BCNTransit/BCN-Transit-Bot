from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional

from src.domain.models.common.connections import Connections
from src.domain.enums.transport_type import TransportType

if TYPE_CHECKING:
    from src.domain.models.common.alert import Alert

@dataclass(kw_only=True)
class Station2:
    id: int 
    code: int
    name: str
    latitude: float
    longitude: float
    order: int
    transport_type: TransportType
    name_with_emoji: Optional[str] = None
    description: Optional[str] = None
    line_id: Optional[int] = None
    line_code: Optional[int] = None
    line_description: Optional[str] = None
    line_color: Optional[str] = None
    line_name: Optional[str] = None
    line_name_with_emoji: Optional[str] = None
    has_alerts: Optional[bool] = False
    
    alerts: Optional[List["Alert"]] = field(default_factory=list)
    connections: Optional[Connections] = None

    @staticmethod
    def get_alert_by_language(station, language: str):
        raw_alerts = []
        if station.has_alerts:
            raw_alerts.extend(
                getattr(alert, f'text{language.capitalize()}')
                for alert in station.alerts
            )
        return "\n".join(f"<pre>{alert}</pre>" for alert in set(raw_alerts))



from src.domain.models.common.line import Line
from src.domain.models.common.alert import Alert
Line.model_rebuild()