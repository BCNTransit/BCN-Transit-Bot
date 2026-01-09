from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from firebase_admin import auth

security = HTTPBearer()

async def get_current_user_uid(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        decoded_token = auth.verify_id_token(token)
        return decoded_token.get('uid') or decoded_token.get('sub') or decoded_token.get('user_id')
    except Exception:
        raise HTTPException(status_code=401, detail="Token inválido")