from fastapi import HTTPException
import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException
import numpy as np
from database.mongo_ops import *
from datetime import datetime, timedelta


async def fetch_ohlcv(email: str, symbol: str, interval: str, start_str: str, end_str: str) -> pd.DataFrame:
    try:
        
        account_response = await get_account_info_by_email(email)
        api_key = account_response["data"]["api_key"]
        secret_key = account_response["data"]["secret_key"]
        client = Client(api_key=api_key, api_secret=secret_key)
        klines = client.get_historical_klines(symbol, interval, start_str, end_str)
        df = pd.DataFrame(klines, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
        ])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
    except BinanceAPIException as e:
        raise HTTPException(status_code=500, detail=str(e))
    

def adjust_start_date(start_date: str, interval: str, window: int = 20) -> str:
    """
    Adjusts the start_date based on the interval and rolling window size
    so indicators like SMA and Bollinger Bands are valid from the original start date.
    
    Args:
        start_date (str): Original start date in 'YYYY-MM-DD' format.
        interval (str): Binance interval string (e.g. '1d', '1h', '15m').
        window (int): Number of periods needed (default is 20 for SMA/Bollinger).
    
    Returns:
        str: Adjusted start date as string in 'YYYY-MM-DD HH:MM:SS' format.
    """
    # Parse the start date
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    
    # Mapping of intervals to timedelta units
    interval_map = {
        '1m': timedelta(minutes=1),
        '3m': timedelta(minutes=3),
        '5m': timedelta(minutes=5),
        '15m': timedelta(minutes=15),
        '30m': timedelta(minutes=30),
        '1h': timedelta(hours=1),
        '2h': timedelta(hours=2),
        '4h': timedelta(hours=4),
        '6h': timedelta(hours=6),
        '8h': timedelta(hours=8),
        '12h': timedelta(hours=12),
        '1d': timedelta(days=1),
        '3d': timedelta(days=3),
        '1w': timedelta(weeks=1),
        '1M': timedelta(days=30)  # Approximate
    }

    if interval not in interval_map:
        raise ValueError(f"Unsupported interval: {interval}")

    adjustment = interval_map[interval] * (window - 1)
    adjusted_dt = start_dt - adjustment

    return adjusted_dt.strftime("%Y-%m-%d %H:%M:%S")
