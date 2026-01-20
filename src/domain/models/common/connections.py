from __future__ import annotations
from typing import List, TYPE_CHECKING
from pydantic import BaseModel, Field, ConfigDict

if TYPE_CHECKING:
    from src.domain.models.common.line import Line

class Connections(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    lines: List["Line"] = Field(default_factory=list)

    def __iter__(self):
        return iter(self.lines)

    def __len__(self):
        return len(self.lines)

    def append(self, line: "Line"):
        self.lines.append(line)