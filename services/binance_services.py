from binance.client import Client
from binance.exceptions import BinanceAPIException
from datetime import datetime
import uuid
import time
import asyncio
from typing import Dict, Any, List, Optional
from database.mongo_ops import *
from services.utils import *
from database.mongo_ops import *

# Helper function to split time range into 24-hour chunks
def split_time_range(start_time: int, end_time: int) -> list[tuple[int, int]]:
    """Split a time range into 24-hour chunks (in milliseconds)."""
    ONE_DAY_MS = 24 * 60 * 60 * 1000  # 24 hours in milliseconds
    ranges = []
    current_start = start_time
    while current_start < end_time:
        current_end = min(current_start + ONE_DAY_MS, end_time)
        ranges.append((current_start, current_end))
        current_start = current_end
    return ranges

# Core function to fetch and store spot account balances
async def fetch_and_store_spot_balances(
    client_name: str,
    account_name: str,
    email: str,
    user_id: str,
    api_key: str,
    secret_key: str
) -> List[Dict[str, Any]]:
    try:
        # Initialize Binance client
        client = Client(api_key=api_key, api_secret=secret_key)
        
        # Synchronize with Binance server time
        server_time = client.get_server_time()
        server_timestamp = server_time['serverTime']
        local_timestamp = int(time.time() * 1000)  # Current local time in milliseconds

        # Calculate time difference
        time_diff = server_timestamp - local_timestamp

        # Adjust client timestamp if necessary
        if abs(time_diff) > 500:  # If time difference is significant (>500ms)
            logger.info(f"Adjusting timestamp by {time_diff}ms to match Binance server time")
            client.timestamp_offset = time_diff

        # Fetch spot account information
        account_info = client.get_account()

        # Extract balances (non-zero balances only)
        balances = [
            {
                "asset": balance["asset"],
                "free": float(balance["free"]),
                "locked": float(balance["locked"])
            }
            for balance in account_info["balances"]
            if float(balance["free"]) > 0 or float(balance["locked"]) > 0
        ]

        # Create a single document for MongoDB
        balance_document = {
            "user_id": user_id,
            "client_name": client_name,
            "account_name": account_name,
            "email": email,
            "balances": balances,
            "timestamp": datetime.utcnow(),
            "document_id": str(uuid.uuid4())
        }

        # Remove existing balances for this account_name and user_id
        spot_data_collection.delete_many({"user_id": user_id})

        print(balance_document)
        # Store balances in MongoDB
        if balances:
            spot_data_collection.insert_one(balance_document)
        print(balances)
        
        spot_balances = []
        for balance in balances: 
            print(balance)
            spot_balances.append({
                "asset": balance["asset"],
                "free": balance["free"],
                "locked": balance["locked"]
            })
        print("Returning spot balances",spot_balances)
        return spot_balances

    except BinanceAPIException as e:
        logger.error(f"Binance API Exception: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error in fetch_and_store_spot_balances: {str(e)}")
        raise

# Core function to fetch and store spot trades
async def fetch_and_store_spot_trades(
    email: str,
    client_name: str,
    account_name: str, 
    user_id: str,
    api_key: str,
    secret_key: str,
    symbol: Optional[str] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: int = 500
) -> List[Dict[str, Any]]:
    try:
        print("In fetch and stores spot trades function")
        print(f"Symbol: {symbol}")
        # Initialize Binance client
        client = Client(api_key=api_key, api_secret=secret_key)
        
        # Synchronize with Binance server time
        server_time = client.get_server_time()
        server_timestamp = server_time['serverTime']
        local_timestamp = int(time.time() * 1000)  # Current local time in milliseconds

        # Calculate time difference
        time_diff = server_timestamp - local_timestamp

        # Adjust client timestamp if necessary
        if abs(time_diff) > 500:  # If time difference is significant (>500ms)
            logger.info(f"Adjusting timestamp by {time_diff}ms to match Binance server time")
            client.timestamp_offset = time_diff

        # Get symbols to query
        symbols = [symbol] if symbol else await get_all_symbols()

        # Check MongoDB for existing trades
        existing_trades = []
        missing_ranges = []
        if start_time and end_time:
            start_dt = datetime.fromtimestamp(start_time / 1000)
            end_dt = datetime.fromtimestamp(end_time / 1000)
            document = trades_collection.find_one({"user_id": user_id})
            if document and "trades" in document:
                # Filter trades within the requested time range
                existing_trades = [
                    trade for trade in document["trades"]
                    if start_dt <= trade["time"] <= end_dt
                    and (not symbol or trade["symbol"] == symbol)
                ]
                # Determine missing time ranges
                existing_times = {trade["time"] for trade in existing_trades}
                time_ranges = split_time_range(start_time, end_time)
                for start, end in time_ranges:
                    start_dt_chunk = datetime.fromtimestamp(start / 1000)
                    end_dt_chunk = datetime.fromtimestamp(end / 1000)
                    # Check if any trade exists in this time chunk
                    has_trades = any(
                        start_dt_chunk <= trade_time <= end_dt_chunk
                        for trade_time in existing_times
                    )
                    if not has_trades:
                        missing_ranges.append((start, end))
        else:
            missing_ranges = [(start_time, end_time)] if start_time and end_time else []

        # Fetch trades from Binance if needed
        new_trades = []
        trade_ids = {trade["id"] for trade in existing_trades}  # Track existing trade IDs
        for symbol in symbols:
            try:
                trade_list = []
                # If no time range or no missing ranges, fetch recent trades
                if not start_time or not end_time or not missing_ranges:
                    params = {"symbol": symbol, "limit": limit}
                    trade_list = client.get_my_trades(**params)
                else:
                    # Fetch trades for missing time ranges
                    for start, end in missing_ranges:
                        params = {
                            "symbol": symbol,
                            "startTime": start,
                            "endTime": end,
                            "limit": limit
                        }
                        chunk_trades = client.get_my_trades(**params)
                        trade_list.extend(chunk_trades)
                        await asyncio.sleep(0.1)  # 100ms delay to respect rate limits

                if not trade_list:
                    continue

                # Prepare trades for storage
                for trade in trade_list:
                    if trade["id"] not in trade_ids:
                        trade_ids.add(trade["id"])
                        new_trades.append({
                            "symbol": trade["symbol"],
                            "id": trade["id"],
                            "orderId": trade["orderId"],
                            "orderListId": trade["orderListId"],
                            "price": float(trade["price"]),
                            "qty": float(trade["qty"]),
                            "quoteQty": float(trade["quoteQty"]),
                            "commission": float(trade["commission"]),
                            "commissionAsset": trade["commissionAsset"],
                            "time": datetime.fromtimestamp(trade["time"] / 1000),
                            "isBuyer": trade["isBuyer"],
                            "isMaker": trade["isMaker"],
                            "isBestMatch": trade["isBestMatch"]
                        })

            except BinanceAPIException as e:
                logger.warning(f"Failed to fetch trades for symbol {symbol}: {str(e)}")
                continue

        # Combine existing and new trades
        all_trades = existing_trades + new_trades

        # Update MongoDB document
        if all_trades:
            # Remove existing document
            trades_collection.delete_many({"user_id": user_id})
            # Create new document
            trade_document = {
                "user_id": user_id,
                "client_name": client_name,
                "email": email,
                "trades": all_trades,
                "timestamp": datetime.utcnow(),
                "document_id": str(uuid.uuid4())
            }
            trades_collection.insert_one(trade_document)

        # Format trades for response
        response_trades = [
            {
                "symbol": trade["symbol"],
                "id": trade["id"],
                "orderId": trade["orderId"],
                "orderListId": trade["orderListId"],
                "price": str(trade["price"]),
                "qty": str(trade["qty"]),
                "quoteQty": str(trade["quoteQty"]),
                "commission": str(trade["commission"]),
                "commissionAsset": trade["commissionAsset"],
                "time": int(trade["time"].timestamp() * 1000),
                "isBuyer": trade["isBuyer"],
                "isMaker": trade["isMaker"],
                "isBestMatch": trade["isBestMatch"]
            }
            for trade in all_trades
        ]
        # if response_trades:
        #     base_response = {
        #         "success": True,
        #         "message": "Fetched spot trades successfully",
        #         "status_code": 200,
        #         "data": response_trades
        #     }
        # else:
        #     base_response ={
        #         "success": True,
        #         "message": "Spot trades are empty",
        #         "status_code": 200,
        #         "data": []
        #     }

        return response_trades

    except Exception as e:
        logger.error(f"Error in fetch_and_store_spot_trades: {str(e)}")
        raise
    
    
# Core function to fetch and store universal transfer history
async def fetch_and_store_universal_transfers(
    client_name: str,
    account_name: str,
    email: str,
    user_id: str,
    api_key: str,
    secret_key: str,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None
) -> Dict[str, Any]:
    try:
        # Initialize Binance client
        client = Client(api_key=api_key, api_secret=secret_key)
        # Synchronize with Binance server time
        server_time = client.get_server_time()
        server_timestamp = server_time['serverTime']
        local_timestamp = int(time.time() * 1000)  # Current local time in milliseconds

        # Calculate time difference
        time_diff = server_timestamp - local_timestamp

        # Adjust client timestamp if necessary
        if abs(time_diff) > 500:  # If time difference is significant (>500ms)
            logger.info(f"Adjusting timestamp by {time_diff}ms to match Binance server time")
            client.timestamp_offset = time_diff

        # List of transfer types from original code
        types_list = [
            'MAIN_UMFUTURE', 'MAIN_CMFUTURE', 'MAIN_MARGIN', 'UMFUTURE_MAIN', 'UMFUTURE_MARGIN',
            'CMFUTURE_MAIN', 'CMFUTURE_MARGIN', 'MARGIN_MAIN', 'MARGIN_UMFUTURE', 'MARGIN_CMFUTURE',
            'MAIN_FUNDING', 'FUNDING_MAIN', 'FUNDING_UMFUTURE', 'UMFUTURE_FUNDING', 'MARGIN_FUNDING',
            'FUNDING_MARGIN', 'FUNDING_CMFUTURE', 'CMFUTURE_FUNDING', 'MAIN_OPTION', 'OPTION_MAIN',
            'UMFUTURE_OPTION', 'OPTION_UMFUTURE', 'MARGIN_OPTION', 'OPTION_MARGIN', 'FUNDING_OPTION',
            'OPTION_FUNDING', 'MAIN_PORTFOLIO_MARGIN', 'PORTFOLIO_MARGIN_MAIN', 'MAIN_ISOLATED_MARGIN'
        ]

        # Skip types as per original code
        skipped_types = [
            'ISOLATEDMARGIN_MARGIN', 'ISOLATEDMARGIN_ISOLATEDMARGIN', 'MARGIN_ISOLATEDMARGIN',
            'ISOLATED_MARGIN_MAIN', 'MAIN_ISOLATED_MARGIN'
        ]

        # Check MongoDB for existing transfers
        existing_transfers = []
        missing_ranges = []
        if start_time and end_time:
            start_dt = datetime.fromtimestamp(start_time / 1000)
            end_dt = datetime.fromtimestamp(end_time / 1000)
            document = transfers_collection.find_one({"user_id": user_id, "client_name": client_name, "account_name": account_name})
            if document and "transfers" in document:
                # Filter transfers within the requested time range
                existing_transfers = [
                    transfer for transfer in document["transfers"]
                    if start_dt <= transfer["timestamp"] <= end_dt
                ]
                # Determine missing time ranges
                existing_times = {transfer["timestamp"] for transfer in existing_transfers}
                time_ranges = split_time_range(start_time, end_time)
                for start, end in time_ranges:
                    start_dt_chunk = datetime.fromtimestamp(start / 1000)
                    end_dt_chunk = datetime.fromtimestamp(end / 1000)
                    # Check if any transfer exists in this time chunk
                    has_transfers = any(
                        start_dt_chunk <= transfer_time <= end_dt_chunk
                        for transfer_time in existing_times
                    )
                    if not has_transfers:
                        missing_ranges.append((start, end))
        else:
            missing_ranges = [(start_time, end_time)] if start_time and end_time else []

        # Fetch transfers from Binance if needed
        new_transfers = []
        tran_ids = {transfer["tranId"] for transfer in existing_transfers}  # Track existing transaction IDs
        for transfer_type in types_list:
            if transfer_type in skipped_types:
                continue

            try:
                transfer_list = []
                # If no time range or no missing ranges, fetch recent transfers
                if not start_time or not end_time or not missing_ranges:
                    params = {"type": transfer_type}
                    transfer_data = client.query_universal_transfer_history(**params)
                    if transfer_data and "rows" in transfer_data:
                        transfer_list = transfer_data["rows"]
                else:
                    # Fetch transfers for missing time ranges
                    for start, end in missing_ranges:
                        params = {
                            "type": transfer_type,
                            "startTime": start,
                            "endTime": end
                        }
                        transfer_data = client.query_universal_transfer_history(**params)
                        if transfer_data and "rows" in transfer_data:
                            transfer_list.extend(transfer_data["rows"])
                        await asyncio.sleep(0.1)  # 100ms delay to respect rate limits

                if not transfer_list:
                    continue

                # Prepare transfers for storage
                for transfer in transfer_list:
                    if transfer["tranId"] not in tran_ids:
                        tran_ids.add(transfer["tranId"])
                        new_transfers.append({
                            "asset": transfer["asset"],
                            "amount": float(transfer["amount"]),
                            "type": transfer["type"],
                            "status": transfer["status"],
                            "tranId": transfer["tranId"],
                            "timestamp": datetime.fromtimestamp(transfer["timestamp"] / 1000)
                        })

            except BinanceAPIException as e:
                logger.warning(f"Failed to fetch transfers for type {transfer_type}: {str(e)}")
                continue

        # Combine existing and new transfers
        all_transfers = existing_transfers + new_transfers

        # Update MongoDB document
        if all_transfers:
            # Remove existing document
            transfers_collection.delete_many({"user_id": user_id, "client_name": client_name, "account_name": account_name})
            # Create new document
            transfer_document = {
                "user_id": user_id,
                "client_name": client_name,
                "account_name": account_name,
                "email": email,
                "transfers": all_transfers,
                "timestamp": datetime.utcnow(),
                "document_id": str(uuid.uuid4())
            }
            transfers_collection.insert_one(transfer_document)

        # Format transfers for response
        # response = {
        #     "total": len(all_transfers),
            # "rows": [
            #     {
            #         "asset": transfer["asset"],
            #         "amount": str(transfer["amount"]),
            #         "type": transfer["type"],
            #         "status": transfer["status"],
            #         "tranId": transfer["tranId"],
            #         "timestamp": int(transfer["timestamp"].timestamp() * 1000)
            #     }
            #     for transfer in all_transfers
            # ]
        # }
        response = [
                {
                    "asset": transfer["asset"],
                    "amount": str(transfer["amount"]),
                    "type": transfer["type"],
                    "status": transfer["status"],
                    "tranId": transfer["tranId"],
                    "timestamp": int(transfer["timestamp"].timestamp() * 1000)
                }
                for transfer in all_transfers
            ]
        return response

    except Exception as e:
        logger.error(f"Error in fetch_and_store_universal_transfers: {str(e)}")
        raise

# Core function to fetch and store futures account information
async def fetch_and_store_futures_account_info(
    client_name: str,
    account_name: str,
    email: str,
    user_id: str,
    api_key: str,
    secret_key: str
) -> Dict[str, Any]:
    try:
        # Initialize Binance client
        client = Client(api_key=api_key, api_secret=secret_key)
        
        # Synchronize with Binance server time
        server_time = client.get_server_time()
        server_timestamp = server_time['serverTime']
        local_timestamp = int(time.time() * 1000)  # Current local time in milliseconds

        # Calculate time difference
        time_diff = server_timestamp - local_timestamp

        # Adjust client timestamp if necessary
        if abs(time_diff) > 500:  # If time difference is significant (>500ms)
            logger.info(f"Adjusting timestamp by {time_diff}ms to match Binance server time")
            client.timestamp_offset = time_diff

        # Fetch futures account information
        acc_info = client.futures_account()

        # Process assets for MongoDB storage
        assets = [
            {
                "asset": asset["asset"],
                "walletBalance": float(asset["walletBalance"]),
                "unrealizedProfit": float(asset["unrealizedProfit"]),
                "marginBalance": float(asset["marginBalance"]),
                "maintMargin": float(asset["maintMargin"]),
                "initialMargin": float(asset["initialMargin"]),
                "positionInitialMargin": float(asset["positionInitialMargin"]),
                "openOrderInitialMargin": float(asset["openOrderInitialMargin"]),
                "crossWalletBalance": float(asset["crossWalletBalance"]),
                "crossUnPnl": float(asset["crossUnPnl"]),
                "availableBalance": float(asset["availableBalance"]),
                "maxWithdrawAmount": float(asset["maxWithdrawAmount"]),
                "marginAvailable": asset["marginAvailable"],
                "updateTime": datetime.fromtimestamp(asset["updateTime"] / 1000) if asset["updateTime"] else datetime.utcnow()
            }
            for asset in acc_info["assets"]
        ]

        # Process positions for MongoDB storage
        positions = [
            {
                "symbol": position["symbol"],
                "initialMargin": float(position["initialMargin"]),
                "maintMargin": float(position["maintMargin"]),
                "unrealizedProfit": float(position["unrealizedProfit"]),
                "positionInitialMargin": float(position["positionInitialMargin"]),
                "openOrderInitialMargin": float(position["openOrderInitialMargin"]),
                "leverage": float(position["leverage"]),
                "isolated": position["isolated"],
                "entryPrice": float(position["entryPrice"]),
                "breakEvenPrice": float(position["breakEvenPrice"]),
                "maxNotional": float(position["maxNotional"]),
                "positionSide": position["positionSide"],
                "positionAmt": float(position["positionAmt"]),
                "notional": float(position["notional"]),
                "isolatedWallet": float(position["isolatedWallet"]),
                "updateTime": datetime.fromtimestamp(position["updateTime"] / 1000) if position["updateTime"] else datetime.utcnow(),
                "bidNotional": float(position["bidNotional"]),
                "askNotional": float(position["askNotional"])
            }
            for position in acc_info["positions"]
        ]

        # Create a single document for MongoDB
        account_document = {
            "user_id": user_id,
            "client_name": client_name,
            "email": email,
            "feeTier": acc_info["feeTier"],
            "feeBurn": acc_info["feeBurn"],
            "canTrade": acc_info["canTrade"],
            "canDeposit": acc_info["canDeposit"],
            "canWithdraw": acc_info["canWithdraw"],
            "updateTime": datetime.fromtimestamp(acc_info["updateTime"] / 1000) if acc_info["updateTime"] else datetime.utcnow(),
            "multiAssetsMargin": acc_info["multiAssetsMargin"],
            "tradeGroupId": acc_info["tradeGroupId"],
            "totalInitialMargin": float(acc_info["totalInitialMargin"]),
            "totalMaintMargin": float(acc_info["totalMaintMargin"]),
            "totalWalletBalance": float(acc_info["totalWalletBalance"]),
            "totalUnrealizedProfit": float(acc_info["totalUnrealizedProfit"]),
            "totalMarginBalance": float(acc_info["totalMarginBalance"]),
            "totalPositionInitialMargin": float(acc_info["totalPositionInitialMargin"]),
            "totalOpenOrderInitialMargin": float(acc_info["totalOpenOrderInitialMargin"]),
            "totalCrossWalletBalance": float(acc_info["totalCrossWalletBalance"]),
            "totalCrossUnPnl": float(acc_info["totalCrossUnPnl"]),
            "availableBalance": float(acc_info["availableBalance"]),
            "maxWithdrawAmount": float(acc_info["maxWithdrawAmount"]),
            "assets": assets,
            "positions": positions,
            "timestamp": datetime.utcnow(),
            "document_id": str(uuid.uuid4())
        }

        # Remove existing document for this account
        futures_account_info_collection.delete_many({"user_id": user_id, "client_name": client_name, "account_name": account_name})

        # Store the single document in MongoDB
        if assets or positions:
            futures_account_info_collection.insert_one(account_document)

        # Format response to match Binance API
        response = {
            "feeTier": acc_info["feeTier"],
            "feeBurn": acc_info["feeBurn"],
            "canTrade": acc_info["canTrade"],
            "canDeposit": acc_info["canDeposit"],
            "canWithdraw": acc_info["canWithdraw"],
            "updateTime": int(float(acc_info["updateTime"])) if acc_info["updateTime"] else 0,
            "multiAssetsMargin": acc_info["multiAssetsMargin"],
            "tradeGroupId": acc_info["tradeGroupId"],
            "totalInitialMargin": f"{float(acc_info['totalInitialMargin']):.8f}",
            "totalMaintMargin": f"{float(acc_info['totalMaintMargin']):.8f}",
            "totalWalletBalance": f"{float(acc_info['totalWalletBalance']):.8f}",
            "totalUnrealizedProfit": f"{float(acc_info['totalUnrealizedProfit']):.8f}",
            "totalMarginBalance": f"{float(acc_info['totalMarginBalance']):.8f}",
            "totalPositionInitialMargin": f"{float(acc_info['totalPositionInitialMargin']):.8f}",
            "totalOpenOrderInitialMargin": f"{float(acc_info['totalOpenOrderInitialMargin']):.8f}",
            "totalCrossWalletBalance": f"{float(acc_info['totalCrossWalletBalance']):.8f}",
            "totalCrossUnPnl": f"{float(acc_info['totalCrossUnPnl']):.8f}",
            "availableBalance": f"{float(acc_info['availableBalance']):.8f}",
            "maxWithdrawAmount": f"{float(acc_info['maxWithdrawAmount']):.8f}",
            "assets": [
                {
                    "asset": asset["asset"],
                    "walletBalance": f"{float(asset['walletBalance']):.8f}",
                    "unrealizedProfit": f"{float(asset['unrealizedProfit']):.8f}",
                    "marginBalance": f"{float(asset['marginBalance']):.8f}",
                    "maintMargin": f"{float(asset['maintMargin']):.8f}",
                    "initialMargin": f"{float(asset['initialMargin']):.8f}",
                    "positionInitialMargin": f"{float(asset['positionInitialMargin']):.8f}",
                    "openOrderInitialMargin": f"{float(asset['openOrderInitialMargin']):.8f}",
                    "crossWalletBalance": f"{float(asset['crossWalletBalance']):.8f}",
                    "crossUnPnl": f"{float(asset['crossUnPnl']):.8f}",
                    "availableBalance": f"{float(asset['availableBalance']):.8f}",
                    "maxWithdrawAmount": f"{float(asset['maxWithdrawAmount']):.8f}",
                    "marginAvailable": asset["marginAvailable"],
                    "updateTime": int(asset["updateTime"].timestamp() * 1000) if isinstance(asset["updateTime"], datetime) else int(float(asset["updateTime"])) if asset["updateTime"] else 0
                }
                for asset in assets
            ],
            "positions": [
                {
                    "symbol": position["symbol"],
                    "initialMargin": f"{float(position['initialMargin']):.8f}",
                    "maintMargin": f"{float(position['maintMargin']):.8f}",
                    "unrealizedProfit": f"{float(position['unrealizedProfit']):.8f}",
                    "positionInitialMargin": f"{float(position['positionInitialMargin']):.8f}",
                    "openOrderInitialMargin": f"{float(position['openOrderInitialMargin']):.8f}",
                    "leverage": f"{float(position['leverage']):.8f}",
                    "isolated": position["isolated"],
                    "entryPrice": f"{float(position['entryPrice']):.8f}",
                    "breakEvenPrice": f"{float(position['breakEvenPrice']):.8f}",
                    "maxNotional": f"{float(position['maxNotional']):.8f}",
                    "positionSide": position["positionSide"],
                    "positionAmt": f"{float(position['positionAmt']):.8f}",
                    "notional": f"{float(position['notional']):.8f}",
                    "isolatedWallet": f"{float(position['isolatedWallet']):.8f}",
                    "updateTime": int(position["updateTime"].timestamp() * 1000) if isinstance(position["updateTime"], datetime) else int(float(position["updateTime"])) if position["updateTime"] else 0,
                    "bidNotional": f"{float(position['bidNotional']):.8f}",
                    "askNotional": f"{float(position['askNotional']):.8f}"
                }
                for position in positions
            ]
        }

        return response

    except BinanceAPIException as e:
        logger.error(f"Binance API Exception: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error in fetch_and_store_futures_account_info: {str(e)}")
        raise
    

# Core function to fetch futures account trades with retries
async def fetch_futures_account_trades(
    client: Client,
    symbol: str,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: int = 500
) -> List[Dict[str, Any]]:
    for attempt in range(5):  # Try up to 5 times
        try:
            params = {
                "symbol": symbol,
                "limit": limit,
                "recvWindow": 10000
            }
            if start_time:
                params["startTime"] = start_time
            if end_time:
                params["endTime"] = end_time

            trades = client.futures_account_trades(**params)
            return trades
        except BinanceAPIException as e:
            logger.warning(f"Attempt {attempt + 1} failed for symbol {symbol}: {str(e)}")
            if attempt < 4:
                await asyncio.sleep(2)  # Wait 2 seconds before retrying
            else:
                raise
    return []

# Core function to fetch and store futures trade list
async def fetch_and_store_futures_trades(
    client_name: str,
    account_name: str,
    user_id: str,
    api_key: str,
    secret_key: str,
    symbol: Optional[str] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: int = 500,
    email: Optional[str] = None
) -> List[Dict[str, Any]]:
    try:
        # Initialize Binance client
        client = Client(api_key=api_key, api_secret=secret_key)
        
        # Synchronize with Binance server time
        server_time = client.get_server_time()
        server_timestamp = server_time['serverTime']
        local_timestamp = int(time.time() * 1000)  # Current local time in milliseconds

        # Calculate time difference
        time_diff = server_timestamp - local_timestamp

        # Adjust client timestamp if necessary
        if abs(time_diff) > 500:  # If time difference is significant (>500ms)
            logger.info(f"Adjusting timestamp by {time_diff}ms to match Binance server time")
            client.timestamp_offset = time_diff

        # Get symbols to query
        symbols = [symbol] if symbol else await get_all_symbols_futures()

        # Check MongoDB for existing trades
        existing_trades = []
        missing_ranges = []
        if start_time and end_time:
            start_dt = datetime.fromtimestamp(start_time / 1000)
            end_dt = datetime.fromtimestamp(end_time / 1000)
            document = futures_trades_collection.find_one({"user_id": user_id, "client_name": client_name, "account_name": account_name})
            if document and "trades" in document:
                # Filter trades within the requested time range
                existing_trades = [
                    trade for trade in document["trades"]
                    if start_dt <= trade["time"] <= end_dt
                    and (not symbol or trade["symbol"] == symbol)
                ]
                # Determine missing time ranges
                existing_times = {trade["time"] for trade in existing_trades}
                time_ranges = split_time_range(start_time, end_time)
                for start, end in time_ranges:
                    start_dt_chunk = datetime.fromtimestamp(start / 1000)
                    end_dt_chunk = datetime.fromtimestamp(end / 1000)
                    # Check if any trade exists in this time chunk
                    has_trades = any(
                        start_dt_chunk <= trade_time <= end_dt_chunk
                        for trade_time in existing_times
                    )
                    if not has_trades:
                        missing_ranges.append((start, end))
        else:
            missing_ranges = [(start_time, end_time)] if start_time and end_time else []

        # Fetch trades from Binance if needed
        new_trades = []
        trade_ids = {trade["id"] for trade in existing_trades}  # Track existing trade IDs
        for symbol in symbols:
            try:
                trade_list = []
                # If no time range or no missing ranges, fetch recent trades
                if not start_time or not end_time or not missing_ranges:
                    trade_list = await fetch_futures_account_trades(
                        client=client,
                        symbol=symbol,
                        limit=limit
                    )
                else:
                    # Fetch trades for missing time ranges
                    for start, end in missing_ranges:
                        chunk_trades = await fetch_futures_account_trades(
                            client=client,
                            symbol=symbol,
                            start_time=start,
                            end_time=end,
                            limit=limit
                        )
                        trade_list.extend(chunk_trades)
                        await asyncio.sleep(0.1)  # 100ms delay to respect rate limits

                if not trade_list:
                    continue

                # Prepare trades for storage
                for trade in trade_list:
                    if trade["id"] not in trade_ids:
                        trade_ids.add(trade["id"])
                        new_trades.append({
                            "symbol": trade["symbol"],
                            "id": trade["id"],
                            "orderId": trade["orderId"],
                            "side": trade["side"],
                            "price": float(trade["price"]),
                            "qty": float(trade["qty"]),
                            "realizedPnl": float(trade["realizedPnl"]),
                            "quoteQty": float(trade["quoteQty"]),
                            "commission": float(trade["commission"]),
                            "commissionAsset": trade["commissionAsset"],
                            "time": datetime.fromtimestamp(trade["time"] / 1000),
                            "positionSide": trade["positionSide"],
                            "buyer": trade["buyer"],
                            "maker": trade["maker"]
                        })

            except BinanceAPIException as e:
                logger.warning(f"Failed to fetch futures trades for symbol {symbol}: {str(e)}")
                continue

        # Combine existing and new trades
        all_trades = existing_trades + new_trades

        # Update MongoDB document
        if all_trades:
            # Remove existing document
            futures_trades_collection.delete_many({"user_id": user_id, "client_name": client_name, "account_name": account_name})
            # Create new document
            trade_document = {
                "user_id": user_id,
                "client_name": client_name,
                "account_name": account_name,
                "email": email,
                "trades": all_trades,
                "timestamp": datetime.utcnow(),
                "document_id": str(uuid.uuid4())
            }
            futures_trades_collection.insert_one(trade_document)

        # Format trades for response
        response_trades = [
            {
                "symbol": trade["symbol"],
                "id": trade["id"],
                "orderId": trade["orderId"],
                "side": trade["side"],
                "price": f"{float(trade['price']):.8f}",
                "qty": f"{float(trade['qty']):.8f}",
                "realizedPnl": f"{float(trade['realizedPnl']):.8f}",
                "quoteQty": f"{float(trade['quoteQty']):.8f}",
                "commission": f"{float(trade['commission']):.8f}",
                "commissionAsset": trade["commissionAsset"],
                "time": int(trade["time"].timestamp() * 1000),
                "positionSide": trade["positionSide"],
                "buyer": trade["buyer"],
                "maker": trade["maker"]
            }
            for trade in all_trades
        ]

        return response_trades

    except Exception as e:
        logger.error(f"Error in fetch_and_store_futures_trades: {str(e)}")
        raise

        
# Core function to fetch and store futures position information
async def fetch_and_store_futures_position_info(
    client_name: str,
    account_name: str,
    user_id: str,
    api_key: str,
    secret_key: str,
    email: Optional[str] = None
) -> List[Dict[str, Any]]:
    try:
        # Initialize Binance client
        client = Client(api_key=api_key, api_secret=secret_key)

        # Synchronize with Binance server time
        server_time = client.get_server_time()
        server_timestamp = server_time['serverTime']
        local_timestamp = int(time.time() * 1000)  # Current local time in milliseconds

        # Calculate time difference
        time_diff = server_timestamp - local_timestamp

        # Adjust client timestamp if necessary
        if abs(time_diff) > 500:  # If time difference is significant (>500ms)
            logger.info(f"Adjusting timestamp by {time_diff}ms to match Binance server time")
            client.timestamp_offset = time_diff

        # Fetch futures position information
        position_info = client.futures_position_information()

        # Process positions for MongoDB storage
        positions = [
            {
                "symbol": position["symbol"],
                "positionSide": position["positionSide"],
                "positionAmt": float(position["positionAmt"]),
                "entryPrice": float(position["entryPrice"]),
                "breakEvenPrice": float(position["breakEvenPrice"]),
                "markPrice": float(position["markPrice"]),
                "unRealizedProfit": float(position["unRealizedProfit"]),
                "liquidationPrice": float(position["liquidationPrice"]),
                "isolatedMargin": float(position["isolatedMargin"]),
                "notional": float(position["notional"]),
                "marginAsset": position["marginAsset"],
                "isolatedWallet": float(position["isolatedWallet"]),
                "initialMargin": float(position["initialMargin"]),
                "maintMargin": float(position["maintMargin"]),
                "positionInitialMargin": float(position["positionInitialMargin"]),
                "openOrderInitialMargin": float(position["openOrderInitialMargin"]),
                "adl": float(position["adl"]),
                "bidNotional": float(position["bidNotional"]),
                "askNotional": float(position["askNotional"]),
                "updateTime": datetime.fromtimestamp(position["updateTime"] / 1000) if position["updateTime"] else datetime.utcnow()
            }
            for position in position_info
        ]

        # Create a single document for MongoDB
        position_document = {
            "user_id": user_id,
            "client_name": client_name,
            "account_name": account_name,
            "email": email,
            "positions": positions,
            "timestamp": datetime.utcnow(),
            "document_id": str(uuid.uuid4())
        }

        # Remove existing document for this account
        futures_position_info_collection.delete_many({"user_id": user_id, "client_name": client_name, "account_name": account_name})

        # Store the single document in MongoDB
        if positions:
            futures_position_info_collection.insert_one(position_document)

        # Format response to match Binance API
        response = [
            {
                "symbol": position["symbol"],
                "positionSide": position["positionSide"],
                "positionAmt": f"{float(position['positionAmt']):.8f}",
                "entryPrice": f"{float(position['entryPrice']):.8f}",
                "breakEvenPrice": f"{float(position['breakEvenPrice']):.8f}",
                "markPrice": f"{float(position['markPrice']):.8f}",
                "unRealizedProfit": f"{float(position['unRealizedProfit']):.8f}",
                "liquidationPrice": f"{float(position['liquidationPrice']):.8f}",
                "isolatedMargin": f"{float(position['isolatedMargin']):.8f}",
                "notional": f"{float(position['notional']):.8f}",
                "marginAsset": position["marginAsset"],
                "isolatedWallet": f"{float(position['isolatedWallet']):.8f}",
                "initialMargin": f"{float(position['initialMargin']):.8f}",
                "maintMargin": f"{float(position['maintMargin']):.8f}",
                "positionInitialMargin": f"{float(position['positionInitialMargin']):.8f}",
                "openOrderInitialMargin": f"{float(position['openOrderInitialMargin']):.8f}",
                "adl": int(float(position["adl"])),  # Ensure adl is an integer
                "bidNotional": f"{float(position['bidNotional']):.8f}",
                "askNotional": f"{float(position['askNotional']):.8f}",
                "updateTime": int(position["updateTime"].timestamp() * 1000) if isinstance(position["updateTime"], datetime) else int(float(position["updateTime"])) if position["updateTime"] else 0
            }
            for position in positions
        ]

        return response

    except BinanceAPIException as e:
        logger.error(f"Binance API Exception: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error in fetch_and_store_futures_position_info: {str(e)}")
        raise

    
# Core function to fetch and store futures account balances
async def fetch_and_store_futures_account_balances(
    client_name: str,
    account_name: str,
    user_id: str,
    api_key: str,
    secret_key: str,
    email: Optional[str] = None
) -> List[Dict[str, Any]]:
    try:
        # Initialize Binance client
        client = Client(api_key=api_key, api_secret=secret_key)

        # Synchronize with Binance server time
        server_time = client.get_server_time()
        server_timestamp = server_time['serverTime']
        local_timestamp = int(time.time() * 1000)  # Current local time in milliseconds

        # Calculate time difference
        time_diff = server_timestamp - local_timestamp

        # Adjust client timestamp if necessary
        if abs(time_diff) > 500:  # If time difference is significant (>500ms)
            logger.info(f"Adjusting timestamp by {time_diff}ms to match Binance server time")
            client.timestamp_offset = time_diff

        # Fetch futures account balances
        futures_balance = client.futures_account_balance()

        # Process balances for MongoDB storage
        balances = [
            {
                "accountAlias": balance["accountAlias"],
                "asset": balance["asset"],
                "balance": float(balance["balance"]),
                "crossWalletBalance": float(balance["crossWalletBalance"]),
                "crossUnPnl": float(balance["crossUnPnl"]),
                "availableBalance": float(balance["availableBalance"]),
                "maxWithdrawAmount": float(balance["maxWithdrawAmount"]),
                "marginAvailable": balance["marginAvailable"],
                "updateTime": datetime.fromtimestamp(balance["updateTime"] / 1000) if balance["updateTime"] else datetime.utcnow()
            }
            for balance in futures_balance
        ]

        # Create a single document for MongoDB
        balance_document = {
            "user_id": user_id,
            "client_name": client_name,
            "account_name": account_name,
            "email": email,
            "balances": balances,
            "timestamp": datetime.utcnow(),
            "document_id": str(uuid.uuid4())
        }

        # Remove existing document for this account
        futures_account_balances_collection.delete_many({"user_id": user_id, "client_name": client_name, "account_name": account_name})

        # Store the single document in MongoDB
        if balances:
            futures_account_balances_collection.insert_one(balance_document)

        # Format response to match Binance API
        response = [
            {
                "accountAlias": balance["accountAlias"],
                "asset": balance["asset"],
                "balance": f"{float(balance['balance']):.8f}",
                "crossWalletBalance": f"{float(balance['crossWalletBalance']):.8f}",
                "crossUnPnl": f"{float(balance['crossUnPnl']):.8f}",
                "availableBalance": f"{float(balance['availableBalance']):.8f}",
                "maxWithdrawAmount": f"{float(balance['maxWithdrawAmount']):.8f}",
                "marginAvailable": balance["marginAvailable"],
                "updateTime": int(balance["updateTime"].timestamp() * 1000) if isinstance(balance["updateTime"], datetime) else int(float(balance["updateTime"])) if balance["updateTime"] else 0
            }
            for balance in balances
        ]

        return response

    except BinanceAPIException as e:
        logger.error(f"Binance API Exception: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error in fetch_and_store_futures_account_balances: {str(e)}")
        raise
