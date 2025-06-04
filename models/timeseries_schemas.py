from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel

class TimeSeriesData(BaseModel):
    timestamps: List[str]
    open: List[float]
    high: List[float]
    low: List[float]
    close: List[float]
    volume: List[float]

class IndicatorData(BaseModel):
    timestamps: List[Optional[str]]
    sma_20: List[Optional[float]]
    ema_20: List[Optional[float]]
    bollinger_upper: List[Optional[float]]
    bollinger_lower: List[Optional[float]]
    close: List[Optional[float]]

class ReturnsData(BaseModel):
    daily_returns: List[Optional[float]]
    cumulative_returns: List[Optional[float]]

class CompareData(BaseModel):
    timestamps: List[str]
    coin1_returns: List[Optional[float]]
    coin2_returns: List[Optional[float]]
    correlation_coefficient: float

class AnomalyPoint(BaseModel):
    timestamp: str
    price: float
    type: str  # 'spike' or 'drop'

class AnomaliesData(BaseModel):
    timestamps: List[str]
    anomaly_points: List[AnomalyPoint]
    
class RSIData(BaseModel):
    timestamps: List[str]
    rsi: List[float]

class MACDData(BaseModel):
    timestamps: List[str]
    macd: List[float]
    signal: List[float]
    histogram: List[float]
    
class StochasticData(BaseModel):
    timestamps: List[str]
    k: List[float]
    d: List[float]

class VWAPData(BaseModel):
    timestamps: List[str]
    vwap: List[float]
    close: List[float]