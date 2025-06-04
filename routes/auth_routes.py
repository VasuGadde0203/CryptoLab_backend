from fastapi import APIRouter, HTTPException
from models.auth_schemas import *
from services.auth_services import *
from database.auth import *

auth_router = APIRouter()

@auth_router.post("/auth/register")
def register(data: RegisterSchema):
    print("In register")
    user_response = register_user(data.email, data.password)
    print(user_response)
    if not user_response:
        raise HTTPException(status_code=user_response["status_code"], detail=user_response["message"])
    if user_response["data"] is None:
        return {"message": user_response["message"]}
    return {"message": "User registered successfully", "user_id": user_response["data"]["user_id"]}

@auth_router.post("/auth/login")
def login(data: LoginSchema):
    user_response = login_user(data.email, data.password)
    if not user_response["success"]:
        raise HTTPException(status_code=user_response["status_code"], detail=user_response["message"])
    
    user_data = user_response["data"]
    token = create_access_token({"email": user_data["email"], "user_id": user_data["user_id"]})

    return {
        "message": "Login successful",
        "access_token": token,
        "user_id": user_data["user_id"]
    }