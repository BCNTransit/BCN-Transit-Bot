import os
import sys
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials
from firebase_admin import auth

security = HTTPBearer()

try:
    SERVER_API_KEY = os.environ["BCN_TRANSIT_API_KEY"]
except KeyError:
    print("❌ CRITICAL ERROR: Environment variable 'BCN_TRANSIT_API_KEY' was not found.")
    print("   The server cannot start without security.")
    sys.exit(1)

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_current_user_uid(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        decoded_token = auth.verify_id_token(token)
        return decoded_token.get('uid') or decoded_token.get('sub') or decoded_token.get('user_id')
    except Exception:
        raise HTTPException(status_code=401, detail="Token inválido")
    
async def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header == SERVER_API_KEY:
        return api_key_header
    
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Could not validate API KEY"
    )