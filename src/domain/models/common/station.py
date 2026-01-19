from __future__ import annotations
from typing import List, Optional, TYPE_CHECKING, Any
from pydantic import BaseModel, Field, ConfigDict

from src.domain.enums.transport_type import TransportType

if TYPE_CHECKING:
    from src.domain.models.common.connections import Connections
    from src.domain.models.common.line import Line
    from src.domain.models.common.alert import Alert

class Station(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    original_id: Optional[str] = None
    code: str
    name: str
    
    latitude: float
    longitude: float
    order: int
    transport_type: TransportType
    
    description: Optional[str] = None
    
    line_id: Optional[str] = None
    line_code: Optional[str] = None
    
    _line_obj: Optional[Any] = None
    station_group_code: Optional[int] = None
    direction: Optional[str] = None
    outboundCode: Optional[str] = None
    returnCode: Optional[str] = None
    moute_id: Optional[str] = None

    has_alerts: bool = False
    alerts: List["Alert"] = Field(default_factory=list)
    connections: Optional["Connections"] = None

    def get_alert_text(self, language: str) -> str:
        if not self.has_alerts:
            return ""
        
        raw_alerts = []
        target_attr = f'text{language.capitalize()}'
        
        for alert in self.alerts:
            text = getattr(alert, target_attr, None)
            if text:
                raw_alerts.append(text)
        
        return "\n".join(f"<pre>{txt}</pre>" for txt in set(raw_alerts))

from src.domain.models.common.line import Line
from src.domain.models.common.connections import Connections
from src.domain.models.common.alert import Alert

Line.model_rebuild()
Connections.model_rebuild()
Station.model_rebuild()