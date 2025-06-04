from pydantic import BaseModel, EmailStr
from typing import List, Dict

class RegisterSchema(BaseModel):
    email: EmailStr
    password: str

class LoginSchema(BaseModel):
    email: EmailStr
    password: str