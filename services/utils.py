import requests
import logging
from fastapi import HTTPException
from typing import Dict, Any, List, Optional

# Binance API base URL
BASE_URL = "https://api.binance.com"

# Binance Futures API base URL
BASE_URL_FUTURES = "https://fapi.binance.com"

logger = logging.getLogger(__name__)

# Helper function to get all trading symbols from Binance
async def get_all_symbols():
    try:
        response = requests.get(f"{BASE_URL}/api/v3/exchangeInfo")
        response.raise_for_status()
        symbols = [symbol["symbol"] for symbol in response.json()["symbols"]]
        return symbols
    except Exception as e:
        logger.error(f"Failed to fetch symbols: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch symbols: {str(e)}")
    
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

# Helper function to get all futures trading symbols from Binance
async def get_all_symbols_futures() -> List[str]:
    try:
        response = requests.get(f"{BASE_URL_FUTURES}/fapi/v1/exchangeInfo")
        response.raise_for_status()
        symbols = [symbol["symbol"] for symbol in response.json()["symbols"]]
        return symbols
    except Exception as e:
        logger.error(f"Failed to fetch futures symbols: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch futures symbols: {str(e)}")