from pydantic import BaseModel, Field, ConfigDict, EmailStr
from datetime import datetime
from typing import List, Optional

class User(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    user_id: str
    username: Optional[str] = None
    created_at: datetime
    language: str
    receive_notifications: bool
    
    email: Optional[EmailStr] = None 
    firebase_uid: Optional[str] = None
    photo_url: Optional[str] = None
    auth_provider: str = "device"

    already_notified: List[int] = Field(default_factory=list)
    fcm_token: str = ""