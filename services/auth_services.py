from database.mongo_ops import *
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from uuid import uuid4

def register_user(email: str, password: str) -> Dict:
    base_response = {
        "message": "Failed to register user",
        "success": False,
        "status_code": 400,
        "data": None
    }
    
    try:
        # Validate inputs
        if not email or not password:
            base_response["message"] = "Email and password are required"
            return base_response

        # Check for existing user
        existing = users_collection.find_one({"email": email})
        if existing:
            base_response["message"] = "User already exists"
            base_response["success"] = True
            base_response["status_code"] = 200
            return base_response

        user = {
            "user_id": str(uuid4()),
            "email": email,
            "password": password,  # Note: In production, hash the password
            "created_at": datetime.utcnow()
        }

        # Insert user with error handling
        result = users_collection.insert_one(user)
        mongo_id = str(result.inserted_id)

        base_response["data"] = {
            "user_id": user["user_id"],
            "email": user["email"],
            "_id": mongo_id
        }
        base_response["message"] = "User registered successfully"
        base_response["success"] = True
        base_response["status_code"] = 200
    except Exception as e:
        base_response["message"] = f"Error registering user: {str(e)}"
        base_response["status_code"] = 500
    return base_response

def login_user(email: str, password: str) -> Dict:
    base_response = {
        "message": "Invalid credentials",
        "success": False,
        "status_code": 401,
        "data": None
    }
    
    try:
        # Validate inputs
        if not email or not password:
            base_response["message"] = "Email and password are required"
            return base_response

        # Find user
        login_response = users_collection.find_one({"email": email, "password": password})
        if not login_response:
            return base_response

        base_response["message"] = "User logged in successfully"
        base_response["success"] = True
        base_response["status_code"] = 200
        base_response["data"] = {
            "user_id": login_response.get("user_id"),
            "email": login_response.get("email"),
            "_id": str(login_response.get("_id"))
        }
    except Exception as e:
        base_response["message"] = f"Error logging in: {str(e)}"
        base_response["status_code"] = 500
    return base_response