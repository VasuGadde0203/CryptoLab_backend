from pymongo import MongoClient
from fastapi import HTTPException

# Connect to MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/vasu")
db = mongo_client["binance_db"]
users_collection = db["users"]
spot_data_collection = db["spot_data"]
accounts_collection = db["client_account_data"]
trades_collection = db["spot_trades"]
transfers_collection = db["universal_transfers"]
futures_account_info_collection = db["futures_account_info"]
futures_trades_collection = db["futures_trades"]
futures_position_info_collection = db["futures_positions_info"]
futures_account_balances_collection = db["futures_account_balances"]
conversations_collection = db["conversations"]

# Helper function to get account info from MongoDB
async def get_account_info(client_name: str,account_name: str):
    base_response = {
        "success": False,
        "status_code": 404,
        "message": "Failed to get account information",
        "data": None
    }
    account = accounts_collection.find_one({"client_name": client_name, "account_name": account_name})
    if not account:
        raise HTTPException(status_code=base_response["status_code"], detail=base_response["message"])
    
    # Explicitly create a JSON-serializable dictionary
    account_data = {
        "client_name": account["client_name"],
        "account_name": account["account_name"],
        "api_key": account["api_key"],
        "secret_key": account["secret_key"]
    }
    
    base_response["success"] = True
    base_response["status_code"] = 200
    base_response["message"] = "Fetch account information successfully"
    base_response["data"] = account_data
    return base_response

# Helper function to get account info from MongoDB
async def get_account_info_by_email(email: str):
    base_response = {
        "success": False,
        "status_code": 404,
        "message": "Failed to get account information",
        "data": None
    }
    account = accounts_collection.find_one({"email": email})
    if not account:
        raise HTTPException(status_code=base_response["status_code"], detail=base_response["message"])
    
    # Explicitly create a JSON-serializable dictionary
    account_data = {
        "email": account["email"],
        "api_key": account["api_key"],
        "secret_key": account["secret_key"]
    }
    
    base_response["success"] = True
    base_response["status_code"] = 200
    base_response["message"] = "Fetch account information successfully"
    base_response["data"] = account_data
    return base_response