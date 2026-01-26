from pydantic import BaseModel, ConfigDict, EmailStr
from datetime import datetime
from typing import Optional

from src.domain.models.common.user_settings import UserSettingsResponse

class User(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    user_id: str
    username: Optional[str] = None
    created_at: datetime
    
    email: Optional[EmailStr] = None 
    firebase_uid: Optional[str] = None
    photo_url: Optional[str] = None
    auth_provider: str = "device"
    
    fcm_token: str = ""
    
    settings: Optional[UserSettingsResponse] = None