from datetime import datetime
from pydantic import BaseModel, ConfigDict

class CardUpdate(BaseModel):
    id: int
    name: str
    expiration_date: datetime

class CardCreate(BaseModel):
    name: str
    expiration_date: datetime

class CardResponse(BaseModel):
    id: int
    name: str
    expiration_date: datetime
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)