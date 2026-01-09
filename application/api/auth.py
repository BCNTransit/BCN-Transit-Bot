from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from firebase_admin import auth

security = HTTPBearer()

async def get_current_user_uid(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        decoded_token = auth.verify_id_token(token)
        
        uid = decoded_token.get('uid')
        
        if not uid:
            uid = decoded_token.get('sub')
            
        if not uid:
            uid = decoded_token.get('user_id')

        return uid
        
    except Exception as e:
        raise HTTPException(...)