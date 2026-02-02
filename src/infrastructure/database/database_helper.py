from datetime import datetime
import uuid
from src.infrastructure.database.database import async_session_factory
from src.domain.schemas.models import DBUser, DBUserCard, DBUserSettings, UserDevice, UserSource


class DatabaseHelper:

    @staticmethod
    async def insert_anonymous_user(session, install_id: str, email=None):
        new_user = DBUser(
            email=email, 
            source=UserSource.ANDROID
        )
        session.add(new_user)
        
        new_device = UserDevice(
            installation_id=install_id,
            fcm_token=str(uuid.uuid4()),
            user=new_user
        )
        
        new_settings = DBUserSettings(user=new_user)
        new_card = DBUserCard(user=new_user, name="Card Anonymous", expiration_date=datetime.utcnow())
        
        session.add(new_device)
        session.add(new_settings)
        session.add(new_card)
        
        await session.flush()
        return new_user
    
    async def insert_registered_user(session, install_id: str, username="test_user", email="test@gmail.com"):
        new_user = DBUser(
            username=username,
            email=email,
            photo_url="http://photo.url",
            firebase_uid="google_uid_123",
            source=UserSource.ANDROID
        )
        session.add(new_user)
        
        new_device = UserDevice(
            installation_id=install_id,
            fcm_token=str(uuid.uuid4()),
            user=new_user
        )
        
        new_settings = DBUserSettings(user=new_user)
        new_card = DBUserCard(user=new_user, name="Card Google", expiration_date=datetime.utcnow())
        
        session.add(new_device)
        session.add(new_settings)
        session.add(new_card)
        
        await session.flush()
        return new_user