from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from movies_data_pipeline.services.auth_service import AuthService
from typing import Dict

router = APIRouter(prefix="/auth", tags=["auth"])
auth_service = AuthService()

@router.post("/token", response_model=Dict[str, str])
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """OAuth2 compatible token login."""
    token_data = auth_service.authenticate_user(form_data.username, form_data.password)
    return token_data