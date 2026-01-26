from typing import Optional
from pydantic import BaseModel, ConfigDict

class UserSettingsUpdate(BaseModel):
    language: Optional[str] = None
    theme_mode: Optional[str] = None
    general_notifications_enabled: Optional[bool] = None
    
    card_alerts_enabled: Optional[bool] = None
    card_alert_days_before: Optional[int] = None
    card_alert_hour: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)


class UserSettingsResponse(BaseModel):
    language: str = "es"
    theme_mode: str = "system"
    general_notifications_enabled: bool = True
    
    card_alerts_enabled: bool = True
    card_alert_days_before: int = 3
    card_alert_hour: int = 9

    model_config = ConfigDict(from_attributes=True)