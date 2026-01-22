import os
import sys
from fastapi import Depends, HTTPException, Header, Security, status
from fastapi.security import APIKeyHeader, HTTPBearer

security = HTTPBearer()

try:
    SERVER_API_KEY = os.environ["BCN_TRANSIT_API_KEY"]
except KeyError:
    print("❌ CRITICAL ERROR: Environment variable 'BCN_TRANSIT_API_KEY' was not found.")
    print("   The server cannot start without security.")
    sys.exit(1)

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_current_user_uid(
    x_user_id: str = Header(..., alias="X-User-Id", description="El installation_id generado por la App Android")
) -> str:
    if not x_user_id:
        raise HTTPException(status_code=400, detail="Header X-User-Id es obligatorio")
    
    return x_user_id
    
async def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header == SERVER_API_KEY:
        return api_key_header
    
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Could not validate API KEY"
    )