from typing import List
from src.domain.models.common.line import Line 
from src.infrastructure.database.repositories.line_repository import LineRepository
from src.domain.enums.transport_type import TransportType

class LineService:
    def __init__(self, repository: LineRepository):
        self.repository = repository

    async def get_all_lines(self) -> List[Line]:
        models = await self.repository.get_all()
        return [Line.model_validate(model) for model in models]

    async def get_lines_by_type(self, t_type: TransportType) -> List[Line]:
        models = await self.repository.get_by_transport_type(t_type.value)
        return [Line.model_validate(model) for model in models]

    async def get_line(self, line_id: str) -> Line | None:
        model = await self.repository.get_by_id(line_id)
        if not model:
            return None
        return Line.model_validate(model)