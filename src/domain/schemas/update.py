from pydantic import BaseModel
from enum import Enum

class UpdateAction(str, Enum):
    NONE = "none"
    RECOMMEND = "recommend"
    FORCE = "force"

class AppVersionResponse(BaseModel):
    action: UpdateAction
    title_key: str
    message_key: str
    store_url: str

    class Config:
        from_attributes = True