from fastapi import APIRouter, Depends
import logging 
from typing import List, Optional
from datetime import datetime
from models.timeseries_schemas import *
from services.timeseries_services import *
from database.auth import *
from database.mongo_ops import *
import requests

timeseries_router = APIRouter()
logger = logging.getLogger(__name__)


@timeseries_router.get("/timeseries/crypto_list", response_model=List[str])
def get_crypto_list(user: dict = Depends(get_current_user)):
    """
    Fetch all USDT trading pairs from Binance API.
    Returns a list of symbols like ["BTCUSDT", "ETHUSDT", ...].
    """
    logger.info(f"User {user.get('email')} requested crypto list")
    print(user)
    print("HELLO")
    
    try:
        # Binance public API endpoint for exchange information
        url = "https://api.binance.com/api/v3/exchangeInfo"
        response = requests.get(url, timeout=5)
        
        # Check if the request was successful
        if response.status_code != 200:
            logger.error(f"Binance API error: {response.status_code} - {response.text}")
            raise HTTPException(status_code=502, detail="Failed to fetch data from Binance API")
        
        data = response.json()
        
        # Extract USDT pairs from symbols
        usdt_pairs = [
            symbol["symbol"]
            for symbol in data.get("symbols", [])
            if symbol["quoteAsset"] == "USDT" and symbol["status"] == "TRADING"
        ]
        
        # Sort the list for consistency
        usdt_pairs.sort()
        
        logger.info(f"Retrieved {len(usdt_pairs)} USDT trading pairs")
        return usdt_pairs
    
    except requests.RequestException as e:
        logger.error(f"Error fetching Binance data: {str(e)}")
        raise HTTPException(status_code=502, detail="Error connecting to Binance API")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@timeseries_router.get("/timeseries/crypto_data", response_model=TimeSeriesData)
async def get_crypto_data(coin: str, interval: str, start_date: str, end_date: str, user: dict = Depends(get_current_user)):
    email = user["email"]
    df = await fetch_ohlcv(email, coin, interval, start_date, end_date)
    return TimeSeriesData(
        timestamps=df['timestamp'].astype(str).tolist(),
        open=df['open'].tolist(),
        high=df['high'].tolist(),
        low=df['low'].tolist(),
        close=df['close'].tolist(),
        volume=df['volume'].tolist()
    )       

@timeseries_router.get("/timeseries/crypto_indicators", response_model=IndicatorData)
async def get_indicators(coin: str, interval: str, start_date: str, end_date: str, user: dict = Depends(get_current_user)):
    email = user["email"]
    new_start_date = adjust_start_date(start_date, interval)

    df = await fetch_ohlcv(email, coin, interval, new_start_date, end_date)
    # Calculate indicators
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['bollinger_std'] = df['close'].rolling(window=20).std()
    df['bollinger_upper'] = df['sma_20'] + (2 * df['bollinger_std'])
    df['bollinger_lower'] = df['sma_20'] - (2 * df['bollinger_std'])

    # Filter to only include rows from the original (user-requested) start_date
    original_start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    df = df[df['timestamp'] >= original_start_datetime]

    return IndicatorData(
        timestamps=df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
        close=df['close'].tolist(),
        sma_20=df['sma_20'].tolist(),
        ema_20=df['ema_20'].tolist(),
        bollinger_upper=df['bollinger_upper'].tolist(),
        bollinger_lower=df['bollinger_lower'].tolist(),
    )

# @timeseries_router.get("/timeseries/crypto_returns", response_model=ReturnsData)
# async def get_returns(coin: str, interval: str, start_date: str, end_date: str, user: dict = Depends(get_current_user)):
#     email = user["email"]
#     df = await fetch_ohlcv(email, coin, interval, start_date, end_date)
#     # Calculate daily and cumulative returns
    
#     df['daily_returns'] = df['close'].pct_change()
#     df['cumulative_returns'] = (1 + df['daily_returns']).cumprod()

#     print(df.isnull().sum())
#     # Handle NaN values by filling them with 0 or dropping
#     df['daily_returns'] = df['daily_returns'].fillna(0)  # Optionally, you can use fillna(0) or dropna()
#     df['cumulative_returns'] = df['cumulative_returns'].fillna(0)

#     print(df.isnull().sum(0))
#     return ReturnsData(
#         daily_returns=df['daily_returns'].round(6).tolist(),
#         cumulative_returns=df['cumulative_returns'].round(6).tolist()
#     )

@timeseries_router.get("/timeseries/crypto_compare", response_model=CompareData)
async def get_comparison(coin1: str, coin2: str, interval: str, start_date: str, end_date: str, user: dict = Depends(get_current_user)):
    email = user["email"]
    df1 = await fetch_ohlcv(email, coin1, interval, start_date, end_date)
    df2 = await fetch_ohlcv(email, coin2, interval, start_date, end_date)
    df1['returns'] = df1['close'].pct_change()
    df2['returns'] = df2['close'].pct_change()
    combined = pd.concat([df1['returns'], df2['returns']], axis=1)
    combined.columns = [coin1, coin2]
    corr = combined.corr().iloc[0, 1]
    df1.dropna(inplace=True)
    df2.dropna(inplace=True)
    return CompareData(
        timestamps=df1['timestamp'].astype(str).tolist(),
        coin1_returns=df1['returns'].round(6).tolist(),
        coin2_returns=df2['returns'].round(6).tolist(),
        correlation_coefficient=round(corr, 4)
    )

# @timeseries_router.get("/timeseries/crypto_anomalies", response_model=AnomaliesData)
# async def get_anomalies(coin: str, interval: str, start_date: str, end_date: str, user: dict = Depends(get_current_user)):
#     email = user["email"]
#     new_start_date = adjust_start_date(start_date, interval)
#     df = await fetch_ohlcv(email, coin, interval, new_start_date, end_date)
#     df['z_score'] = (df['close'] - df['close'].rolling(20).mean()) / df['close'].rolling(20).std()
#     anomalies = df[(df['z_score'].abs() > 2)]
#     anomaly_points = [
#         AnomalyPoint(
#             timestamp=str(row['timestamp']),
#             price=row['close'],
#             type='spike' if row['z_score'] > 2 else 'drop'
#         ) for _, row in anomalies.iterrows()
#     ]
#     return AnomaliesData(
#         timestamps=df['timestamp'].astype(str).tolist(),
#         anomaly_points=anomaly_points
#     )

# Crypto RSI endpoint (replacing Returns)
@timeseries_router.get("/timeseries/crypto_rsi", response_model=RSIData)
async def get_rsi(coin: str, interval: str, start_date: str, end_date: str, user: dict = Depends(get_current_user)):
    email = user["email"]
    new_start_date = adjust_start_date(start_date, interval)
    df = await fetch_ohlcv(email, coin, interval, new_start_date, end_date)
    
    # Calculate RSI (14-period)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # Filter to original start date
    original_start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    df = df[df['timestamp'] >= original_start_datetime]
    
    # Handle NaN values
    df['rsi'] = df['rsi'].fillna(50)  # Default to neutral RSI if NaN
    
    return RSIData(
        timestamps=df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
        rsi=df['rsi'].round(4).tolist()
    )

# Crypto MACD endpoint (replacing Anomalies)
@timeseries_router.get("/timeseries/crypto_macd", response_model=MACDData)
async def get_macd(coin: str, interval: str, start_date: str, end_date: str, user: dict = Depends(get_current_user)):
    email = user["email"]
    new_start_date = adjust_start_date(start_date, interval)
    df = await fetch_ohlcv(email, coin, interval, new_start_date, end_date)
    
    # Calculate MACD
    ema_12 = df['close'].ewm(span=12, adjust=False).mean()
    ema_26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = ema_12 - ema_26
    df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['histogram'] = df['macd'] - df['signal']
    
    # Filter to original start date
    original_start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    df = df[df['timestamp'] >= original_start_datetime]
    
    # Handle NaN values
    df['macd'] = df['macd'].fillna(0)
    df['signal'] = df['signal'].fillna(0)
    df['histogram'] = df['histogram'].fillna(0)
    
    return MACDData(
        timestamps=df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
        macd=df['macd'].round(4).tolist(),
        signal=df['signal'].round(4).tolist(),
        histogram=df['histogram'].round(4).tolist()
    )

# Crypto Stochastic endpoint (new)
@timeseries_router.get("/timeseries/crypto_stochastic", response_model=StochasticData)
async def get_stochastic(coin: str, interval: str, start_date: str, end_date: str, user: dict = Depends(get_current_user)):
    email = user["email"]
    new_start_date = adjust_start_date(start_date, interval)
    df = await fetch_ohlcv(email, coin, interval, new_start_date, end_date)
    
    # Calculate Stochastic Oscillator (%K and %D)
    df['low_14'] = df['low'].rolling(window=14).min()
    df['high_14'] = df['high'].rolling(window=14).max()
    df['k'] = 100 * (df['close'] - df['low_14']) / (df['high_14'] - df['low_14'])
    df['d'] = df['k'].rolling(window=3).mean()
    
    # Filter to original start date
    original_start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    df = df[df['timestamp'] >= original_start_datetime]
    
    # Handle NaN values
    df['k'] = df['k'].fillna(50)
    df['d'] = df['d'].fillna(50)
    
    return StochasticData(
        timestamps=df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
        k=df['k'].round(4).tolist(),
        d=df['d'].round(4).tolist()
    )

# Crypto VWAP endpoint (new)
@timeseries_router.get("/timeseries/crypto_vwap", response_model=VWAPData)
async def get_vwap(coin: str, interval: str, start_date: str, end_date: str, user: dict = Depends(get_current_user)):
    email = user["email"]
    new_start_date = adjust_start_date(start_date, interval)
    df = await fetch_ohlcv(email, coin, interval, new_start_date, end_date)
    
    # Calculate VWAP
    df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
    df['price_volume'] = df['typical_price'] * df['volume']
    # For intraday intervals, reset VWAP daily
    if interval in ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h"]:
        df['date'] = df['timestamp'].dt.date
        df['cum_pv'] = df.groupby('date')['price_volume'].cumsum()
        df['cum_volume'] = df.groupby('date')['volume'].cumsum()
    else:
        df['cum_pv'] = df['price_volume'].cumsum()
        df['cum_volume'] = df['volume'].cumsum()
    df['vwap'] = df['cum_pv'] / df['cum_volume']
    
    # Filter to original start date
    original_start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    df = df[df['timestamp'] >= original_start_datetime]
    
    # HandleインプットHandle NaN values
    df['vwap'] = df['vwap'].fillna(df['close'])  # Use close price if VWAP is NaN
    df['close'] = df['close'].fillna(df['close'].mean())  # Fallback to mean close
    
    return VWAPData(
        timestamps=df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
        vwap=df['vwap'].round(4).tolist(),
        close=df['close'].round(4).tolist()
    )