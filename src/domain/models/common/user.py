from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

@dataclass
class User:
    user_id: str  # Esto mapea al 'external_id' o al 'id' interno según tu lógica
    username: Optional[str] # Ahora puede ser None si viene de Google sin username definido
    created_at: datetime
    language: str
    receive_notifications: bool
    
    # --- NUEVOS CAMPOS ---
    email: Optional[str] = None
    firebase_uid: Optional[str] = None
    photo_url: Optional[str] = None
    auth_provider: str = "device"
    # ---------------------

    already_notified: List[int] = field(default_factory=list)
    fcm_token: str = ""
