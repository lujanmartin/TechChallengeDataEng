from jose import JWTError, jwt
from datetime import datetime, timedelta
from passlib.context import CryptContext
from fastapi import HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer
from movies_data_pipeline.config.auth import (
    SECRET_KEY,
    ALGORITHM,
    ACCESS_TOKEN_EXPIRE_MINUTES,
)
import logging
import os

logger = logging.getLogger(__name__)

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


HARDCODED_USERNAME = os.getenv("HARDCODED_USERNAME")
HARDCODED_PASSWORD = os.getenv("HARDCODED_PASSWORD")

HARDCODED_PASSWORD_HASH = pwd_context.hash(HARDCODED_PASSWORD)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token")


class AuthService:
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        return pwd_context.verify(plain_password, hashed_password)

    def create_access_token(self, data: dict, expires_delta: timedelta = None) -> str:
        to_encode = data.copy()
        expire = datetime.utcnow() + (
            expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt

    def authenticate_user(self, username: str, password: str) -> dict:
        if username != HARDCODED_USERNAME or not self.verify_password(
            password, HARDCODED_PASSWORD_HASH
        ):
            logger.warning(f"Authentication failed for user: {username}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        access_token = self.create_access_token(data={"sub": username})
        return {"access_token": access_token, "token_type": "bearer"}


async def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return username
