

from dataclasses import dataclass
from src.domain.models.common.line import Line


@dataclass
class Connections:
    lines: list[Line]