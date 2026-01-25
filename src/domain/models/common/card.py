from datetime import datetime
from pydantic import BaseModel


class CardCreate(BaseModel):
    name: str
    expiration_date: datetime

class CardResponse(BaseModel):
    id: int
    name: str
    expiration_date: datetime
    created_at: datetime

    class Config:
        from_attributes = True