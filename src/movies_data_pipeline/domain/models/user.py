from pydantic import BaseModel


class User(BaseModel):
    username: str
    password: str  # Plaintext for input, hashed in DB
