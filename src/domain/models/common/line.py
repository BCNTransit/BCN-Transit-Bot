from __future__ import annotations
from typing import Optional, List, TYPE_CHECKING
from pydantic import BaseModel, computed_field, Field

from src.domain.enums.transport_type import TransportType

if TYPE_CHECKING:
    from src.domain.models.common.alert import Alert

class Line(BaseModel):
    id: str
    code: str
    name: str
    description: Optional[str] = None
    origin: Optional[str] = None
    destination: Optional[str] = None
    color: str = None
    transport_type: TransportType
    category: Optional[str] = None 
    stations: List["Station"] = Field(default_factory=list)    
    has_alerts: bool = False
    alerts: List["Alert"] = Field(default_factory=list)

    class Config:
        from_attributes = True

    @computed_field
    def name_with_emoji(self) -> str:
        emoji = self._get_emoji()
        return f"{emoji} {self.name}"

    def _get_emoji(self) -> str:
        tt = self.transport_type
        name = self.name
        
        if tt == TransportType.METRO:
            mapping = {
                "L1": "🟥", "L2": "🟪", "L3": "🟩", "L4": "🟨", "L5": "🟦",
                "L9N": "🟧", "L9S": "🟧", "L10N": "🟦", "L10S": "🟦", "L11": "🟩"
            }
            return mapping.get(name, "🚇")

        if tt == TransportType.TRAM:
            return "🟩" if name.startswith("T") else "🚃"

        if tt == TransportType.FGC:
            mapping = {
                "L1": "🟥", "S1": "🟥", "S2": "🟩", "L6": "🟪", "L7": "🟫", "L12": "🟪",
                "L8": "🟪", "S3": "🟦", "S4": "🟨", "S8": "🟦", "S9": "🟥",
                "R5": "🟦", "R50": "🟦", "R6": "⬛", "R60": "⬛", "R63": "⬛",
                "RL1": "🟩", "RL2": "🟩"
            }
            return mapping.get(name, "🚂")

        if tt == TransportType.RODALIES:
            mapping = {
                "R1": "🟦", "R2": "🟩", "R2 Nord": "🟩", "R2 Sud": "🟩",
                "R3": "🟥", "R4": "🟨", "R7": "⬜", "R8": "🟪", "R11": "🟦",
                "R13": "⬛", "R14": "🟪", "R15": "🟫", "R16": "🟥", "R17": "🟧",
                "RG1": "🟦", "RT1": "🟦", "RT2": "⬜", "RL3": "🟩", "RL4": "🟨"
            }
            return mapping.get(name, "🚆")

        if tt == TransportType.BUS:
            if name.isdigit(): return "🔴"
            if name.startswith("H"): return "🟦"
            if name.startswith("D"): return "🟪"
            if name.startswith("V"): return "🟩"
            if name.startswith("M"): return "🔴"
            if name.startswith("X"): return "⚫"
            return "🚌"

        return ""
    
from src.domain.models.common.station import Station
from src.domain.models.common.alert import Alert
Line.model_rebuild()