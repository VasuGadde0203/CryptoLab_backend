from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from binance.client import Client
from binance.exceptions import BinanceAPIException
from pymongo import MongoClient
from models.endpoints_schemas import *
from database.mongo_ops import *
from datetime import datetime
import logging
import uuid
from database.auth import *
from services.utils import *
from services.binance_services import *

binance_router = APIRouter()
logger = logging.getLogger(__name__)

# FastAPI endpoint for spot account information
@binance_router.post("/spot/account-information")
async def spot_account_information(request: SpotAccountRequest, user: dict = Depends(get_current_user)):
    base_response = {
        "success": False,
        "status_code": 400,
        "message": "Failed to get spot account information",
        "data": None
    }
    try:
        print(user)
        user_id = user["user_id"]
        logger.info(f"Fetching spot account information for user_id: {user_id}")

        client_name = request.client_name
        account_name = request.account_name
        # Fetch account info from MongoDB
        account_response = await get_account_info(client_name, account_name)
        logger.info("Fetched account information and API keys")
        print(account_response["data"])
        client_name = account_response["data"]["client_name"]
        api_key = account_response["data"]["api_key"]
        secret_key = account_response["data"]["secret_key"]

        print("Fetching and storing balances")
        # Fetch and store balances
        balances = await fetch_and_store_spot_balances(
            client_name= client_name,
            account_name= account_name,
            email=user["email"],
            user_id=user_id,
            api_key=api_key,
            secret_key=secret_key
        )

        # print(balances)
        
        if balances:
            # Prepare response
            base_response = {
                "success": True,
                "status_code": 200,
                "message": "Successfully fetched spot account information data",
                "data": {
                    "client_name": client_name,
                    "balances": balances
                }
            }
        else:
            # Prepare response
            base_response = {
                "success": True,
                "status_code": 200,
                "message": f"spot account information data is empty for {account_name}",
                "data": {
                    "client_name": client_name,
                    "balances": balances
                }
            }
            
        return base_response

    except BinanceAPIException as e:
        logger.error(f"Binance API Exception: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Binance API error: {str(e)}")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
# FastAPI endpoint for spot trade list
@binance_router.post("/spot/trade-list")
async def spot_trade_list(request: TradeListRequest, user: dict = Depends(get_current_user)):
    base_response = {
        "success": False,
        "status_code": 400,
        "message": "Failed to fetch spot trades",
        "data": None
    }
    try:
        # Validate limit
        if request.limit > 1000:
            base_response["message"] = "Limit cannot exceed 1000"
            raise HTTPException(status_code=base_response["status_code"], detail=base_response["message"])

        client_name = request.client_name
        account_name = request.account_name
        
        # Fetch account info from MongoDB
        account_response = await get_account_info(client_name, account_name)
        # client_name = account_response["data"]["client_name"]
        api_key = account_response["data"]["api_key"]
        secret_key = account_response["data"]["secret_key"]
        user_id = user["user_id"]

        # Fetch and store trades
        spot_trades = await fetch_and_store_spot_trades(
            email=user["email"],
            client_name=client_name,
            account_name=account_name,
            user_id=user_id,
            api_key=api_key,
            secret_key=secret_key,
            symbol=request.symbol,
            start_time=request.start_time,
            end_time=request.end_time,
            limit=request.limit
        )
        if spot_trades:
                # Prepare response
            base_response = {
                "success": True,
                "status_code": 200,
                "message": f"Trades for {request.account_name} stored successfully",
                "data": {
                    "client_name": request.account_name,
                    "trades": spot_trades
                }
            }
        else:
            base_response ={
                "success": True,
                "status_code": 200,
                "message": f"Trades for {request.account_name} is empty",
                "data": {
                    "client_name": request.account_name,
                    "trades": spot_trades
                }
            }

        return base_response

    except BinanceAPIException as e:
        logger.error(f"Binance API Exception: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Binance API error: {str(e)}")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    

# FastAPI endpoint for universal transfer history
@binance_router.post("/spot/universal-transfer-history")
async def spot_universal_transfer_history(
    request: UniversalTransferRequest,
    user: dict = Depends(get_current_user)
):
    base_response = {
        "success": False,
        "status_code": 400,
        "message": "Failed to fetch universal transfer history",
        "data": None
    }
    try:
        user_id = user["user_id"]
        logger.info(f"Fetching universal transfer history for user_id: {user_id}")
        
        client_name = request.client_name
        account_name = request.account_name

        # Fetch account info from MongoDB
        account_response = await get_account_info(client_name, account_name)
        logger.info("Fetched account information and API keys")

        client_name = account_response["data"]["client_name"]
        api_key = account_response["data"]["api_key"]
        secret_key = account_response["data"]["secret_key"]

        # Fetch and store transfers
        transfers = await fetch_and_store_universal_transfers(
            client_name=client_name,
            account_name=account_name,
            email=user["email"],
            user_id=user_id,
            api_key=api_key,
            secret_key=secret_key,
            start_time=request.start_time,
            end_time=request.end_time
        )

        if transfers:
            # Prepare response
            base_response = {
                "success": True,
                "status_code": 200,
                "message": f"Universal transfers for {account_name} stored successfully",
                "data": {
                    "client_name": account_name,
                    "transfers": transfers
                }
            }
        
        else:
            # Prepare response
            base_response = {
                "success": True,
                "status_code": 200,
                "message": f"Universal transfers for {account_name} is empty",
                "data": {
                    "client_name": account_name,
                    "transfers": transfers
                }
            }
        return base_response

    except BinanceAPIException as e:
        logger.error(f"Binance API Exception: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Binance API error: {str(e)}")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
# FastAPI endpoint for futures account information
@binance_router.post("/futures/account-information")
async def futures_account_information(
    request: FuturesAccountRequest,
    user: dict = Depends(get_current_user)
):
    base_response = {
        "success": False,
        "status_code": 400,
        "message": "Failed to fetch futures account information",
        "data": None
    }
    try:
        user_id = user["user_id"]
        logger.info(f"Fetching futures account information for user_id: {user_id}")
        
        client_name = request.client_name
        account_name = request.account_name

        # Fetch account info from MongoDB
        account_response = await get_account_info(client_name, account_name)
        logger.info("Fetched account information and API keys")

        client_name = account_response["data"]["client_name"]
        api_key = account_response["data"]["api_key"]
        secret_key = account_response["data"]["secret_key"]

        # Fetch and store futures account information
        account_data = await fetch_and_store_futures_account_info(
            client_name=client_name,
            account_name=account_name,
            email=user["email"],
            user_id=user_id,
            api_key=api_key,
            secret_key=secret_key
        )

        if account_data:
            # Prepare response
            base_response = {
                "success": True,
                "status_code": 200,
                "message": f"Futures account information for {account_name} stored successfully",
                "data": {
                    "account_name": account_name,
                    "assets": account_data["assets"],
                    "positions": account_data["positions"]
                }
            }
        
        else:
            # Prepare response
            base_response = {
                "success": True,
                "status_code": 200,
                "message": f"Futures account information for {account_name} is empty",
                "data": {
                    "account_name": account_name,
                    "assets": account_data["assets"],
                    "positions": account_data["positions"]
                }
            }
            
        return base_response

    except BinanceAPIException as e:
        logger.error(f"Binance API Exception: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Binance API error: {str(e)}")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
# FastAPI endpoint for futures trade list
@binance_router.post("/futures/trade-list")
async def futures_trade_list(
    request: FuturesTradeListRequest,
    user: dict = Depends(get_current_user)
):
    base_response = {
        "success": False,
        "status_code": 400,
        "message": "Failed to fetch futures trade list",
        "data": None
    }
    try:
        # Validate limit
        if request.limit > 1000:
            base_response["message"] = "Limit cannot exceed 1000"
            raise HTTPException(status_code=base_response["status_code"], detail=base_response["message"])

        user_id = user["user_id"]
        logger.info(f"Fetching futures trade list for user_id: {user_id}")

        client_name = request.client_name
        account_name = request.account_name

        # Fetch account info from MongoDB
        account_response = await get_account_info(client_name, account_name)
        logger.info("Fetched account information and API keys")

        client_name = account_response["data"]["client_name"]
        api_key = account_response["data"]["api_key"]
        secret_key = account_response["data"]["secret_key"]

        # Fetch and store trades
        trades = await fetch_and_store_futures_trades(
            client_name=client_name,
            account_name=account_name,
            email=user["email"],
            user_id=user_id,
            api_key=api_key,
            secret_key=secret_key,
            symbol=request.symbol,
            start_time=request.start_time,
            end_time=request.end_time,
            limit=request.limit
        )

        if trades:
            # Prepare response
            base_response = {
                "success": True,
                "status_code": 200,
                "message": f"Futures trade list for {client_name} stored successfully",
                "data": {
                    "account_name": account_name,
                    "trades": trades
                }
            }
        else:
            # Prepare response
            base_response = {
                "success": True,
                "status_code": 200,
                "message": f"Futures trade list for {client_name} is empty",
                "data": {
                    "account_name": account_name,
                    "trades": trades
                }
            }
            
        return base_response

    except BinanceAPIException as e:
        logger.error(f"Binance API Exception: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Binance API error: {str(e)}")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
# FastAPI endpoint for futures position information
@binance_router.post("/futures/position-information")
async def futures_position_information(
    request: FuturesPositionInfoRequest,
    user: dict = Depends(get_current_user)
):
    base_response = {
        "success": False,
        "status_code": 400,
        "message": "Failed to fetch futures position information",
        "data": None
    }
    try:
        user_id = user["user_id"]
        logger.info(f"Fetching futures position information for user_id: {user_id}")

        client_name = request.client_name
        account_name = request.account_name

        # Fetch account info from MongoDB
        account_response = await get_account_info(client_name, account_name)
        logger.info("Fetched account information and API keys")

        client_name = account_response["data"]["client_name"]
        api_key = account_response["data"]["api_key"]
        secret_key = account_response["data"]["secret_key"]

        # Fetch and store position information
        positions = await fetch_and_store_futures_position_info(
            client_name=client_name,
            account_name=account_name,
            email=user["email"],
            user_id=user_id,
            api_key=api_key,
            secret_key=secret_key
        )

        if positions:
            # Prepare response
            base_response = {
                "success": True,
                "status_code": 200,
                "message": f"Futures position information for {client_name} stored successfully",
                "data": {
                    "account_name": account_name,
                    "positions": positions
                }
            }
        else:
            # Prepare response
            base_response = {
                "success": True,
                "status_code": 200,
                "message": f"Futures position information for {account_name} is empty",
                "data": {
                    "account_name": account_name,
                    "positions": positions
                }
            }
            
        return base_response

    except BinanceAPIException as e:
        logger.error(f"Binance API Exception: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Binance API error: {str(e)}")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
# FastAPI endpoint for futures account balances
@binance_router.post("/futures/account-balances")
async def futures_account_balances(
    request: FuturesAccountBalancesRequest,
    user: dict = Depends(get_current_user)
):
    base_response = {
        "success": False,
        "status_code": 400,
        "message": "Failed to fetch futures account balances",
        "data": None
    }
    try:
        user_id = user["user_id"]
        logger.info(f"Fetching futures account balances for user_id: {user_id}")

        client_name = request.client_name
        account_name = request.account_name

        # Fetch account info from MongoDB
        account_response = await get_account_info(client_name, account_name)
        logger.info("Fetched account information and API keys")

        client_name = account_response["data"]["client_name"]
        api_key = account_response["data"]["api_key"]
        secret_key = account_response["data"]["secret_key"]

        # Fetch and store account balances
        balances = await fetch_and_store_futures_account_balances(
            client_name=client_name,
            account_name=account_name,
            user_id=user_id,
            api_key=api_key,
            secret_key=secret_key
        )

        if balances:
            # Prepare response
            base_response = {
                "success": True,
                "status_code": 200,
                "message": f"Futures account balances for {account_name} stored successfully",
                "data": {
                    "account_name": account_name,
                    "balances": balances
                }
            }
        else:
            # Prepare response
            base_response = {
                "success": True,
                "status_code": 200,
                "message": f"Futures account balances for {account_name} is empty",
                "data": {
                    "account_name": account_name,
                    "balances": balances
                }
            }
            
        return base_response

    except BinanceAPIException as e:
        logger.error(f"Binance API Exception: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Binance API error: {str(e)}")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")