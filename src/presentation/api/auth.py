import sys
from typing import Optional
from fastapi import Depends, HTTPException, Header, Request, Security, status
from fastapi.security import APIKeyHeader, HTTPBearer
from src.application.services.user_data_manager import UserDataManager
from firebase_admin import auth

security = HTTPBearer()

try:
    SERVER_API_KEY = "2ca27e01-7cd6-4e4e-b234-f19827ebfaa5"
except KeyError:
    print("❌ CRITICAL ERROR: Environment variable 'BCN_TRANSIT_API_KEY' was not found.")
    print("   The server cannot start without security.")
    sys.exit(1)

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_current_user_uid(
    request: Request,
    user_data_manager: UserDataManager = Depends(),
    x_user_id: Optional[str] = Header(None, alias="X-User-Id"),
) -> int:
    
    auth_header = request.headers.get("Authorization")
    
    if auth_header and auth_header.startswith("Bearer "):
        try:
            token = auth_header.split(" ")[1]
            decoded_token = auth.verify_id_token(token)
            google_uid = decoded_token['uid']
            
            user_id = await user_data_manager.get_user_id_by_google_uid(google_uid)
            
            if user_id:
                return user_id
                
        except Exception as e:
            print(f"Token inválido: {e}")
            pass

    if x_user_id:
        user_id = await user_data_manager.get_user_id_by_installation_id(x_user_id)
        
        if user_id:
            return user_id

    raise HTTPException(status_code=401, detail="Credenciales no válidas o usuario no encontrado")
    
    
async def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header == SERVER_API_KEY:
        return api_key_header
    
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Could not validate API KEY"
    )