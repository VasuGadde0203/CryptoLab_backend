from fastapi import FastAPI, HTTPException, APIRouter, Depends
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from fastapi.responses import JSONResponse
import numpy as np
from models.forecast_schemas import *
from database.auth import *
import logging
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import load_model
import pytz

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

forecast_router = APIRouter()

# Constants (same as in your training script)
LOOK_BACK = 120
TICKER_SYMBOL = "BTC-USD"
INTERVAL = "1h"

# Load the pre-trained LSTM model
model = load_model('trained_models/lstm_btc_model.h5')
scaler = MinMaxScaler(feature_range=(0, 1))

@forecast_router.post("/api/forecast")
async def get_forecast(request: forecast_request, user: dict = Depends(get_current_user)):
    try:
        print("Forecasting")
        start_date = request.start_date
        end_date = request.end_date
        try:
            forecast_start = datetime.strptime(start_date, "%Y-%m-%d")
            forecast_end = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            logger.error(f"Invalid date format: start_date={start_date}, end_date={end_date}")
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
        
        # Validate forecast date logic
        current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if forecast_start > forecast_end:
            logger.error(f"End date {end_date} is before start date {start_date}")
            raise HTTPException(status_code=400, detail="End date must be after start date.")
        if forecast_start < current_date:
            logger.error(f"Start date {start_date} is in the past")
            raise HTTPException(status_code=400, detail="Start date cannot be in the past.")
        
        # Fetch historical Bitcoin data (last 400 days for context, but ensure enough for LOOK_BACK)
        historical_start = current_date - timedelta(days=400)
        logger.info(f"Fetching BTC-USD data from {historical_start} to {current_date}")
        btc = yf.download(TICKER_SYMBOL, start=historical_start, end=current_date + timedelta(days=1), interval=INTERVAL)
        
        if btc.empty:
            logger.error("No historical data returned from yfinance.")
            raise HTTPException(status_code=404, detail="No historical data available for the model.")
        
        # Prepare historical data
        prices = btc["Close"]
        if isinstance(prices, pd.DataFrame):
            logger.warning(f"Prices is a DataFrame, converting to Series. Columns: {prices.columns.tolist()}")
            if len(prices.columns) == 1:
                prices = prices.iloc[:, 0]
            else:
                logger.error(f"Prices DataFrame has multiple columns: {prices.columns.tolist()}")
                raise HTTPException(status_code=500, detail="Unexpected multi-column DataFrame for prices.")
        
        prices = prices.dropna()
        if prices.empty:
            logger.error("Prices Series is empty after dropping NaN values.")
            raise HTTPException(status_code=404, detail="No valid price data available after processing.")
        
        # Ensure we have enough data for the LOOK_BACK window
        if len(prices) < LOOK_BACK:
            logger.error(f"Not enough data for LOOK_BACK window: {len(prices)} < {LOOK_BACK}")
            raise HTTPException(status_code=400, detail="Not enough historical data for forecasting.")
        
        # Slice the last 30 days of historical data for the response (convert to daily for consistency)
        last_30_days_start = current_date - timedelta(days=30)
        last_30_days_start = last_30_days_start.replace(tzinfo=pytz.UTC)
        prices_last_30_days = prices[prices.index >= last_30_days_start].resample('D').mean().dropna()
        historical_dates = prices_last_30_days.index.strftime("%Y-%m-%d").tolist()
        historical_prices = prices_last_30_days.values.tolist()
        logger.info(f"Historical data points (last 30 days): {len(historical_prices)}")
        
        # Check for variability in historical data
        price_std = np.std(historical_prices)
        if price_std < 1e-5:
            logger.warning("Historical prices show very low variability.")
        
        # Calculate historical statistics
        hist_stats = {
            "mean": float(prices_last_30_days.mean()),
            "std": float(prices_last_30_days.std()),
            "min": float(prices_last_30_days.min()),
            "max": float(prices_last_30_days.max())
        }
        logger.info(f"Historical stats: {hist_stats}")
        
        # Prepare data for LSTM prediction
        closing_prices = prices.values.reshape(-1, 1)
        scaler.fit(closing_prices)  # Fit the scaler on all historical data
        scaled_data = scaler.transform(closing_prices)

        # Create the last window for prediction
        last_window = scaled_data[-LOOK_BACK:].reshape((1, LOOK_BACK, 1))
        
        # Calculate forecast steps (daily forecast)
        days_to_start = (forecast_start - current_date).days
        days_to_end = (forecast_end - current_date).days
        forecast_steps = days_to_end - days_to_start + 1  # Include both start and end dates
        logger.debug(f"Days to start: {days_to_start}, Days to end: {days_to_end}, Forecast steps: {forecast_steps}")
        
        if forecast_steps <= 0:
            logger.error(f"Invalid forecast period: {forecast_steps} steps")
            raise HTTPException(status_code=400, detail="Invalid forecast period.")
        
        # Iterative forecasting with LSTM
        predictions = []
        current_window = last_window.copy()
        for _ in range(forecast_steps * 24):  # Convert days to hours since model was trained on hourly data
            pred = model.predict(current_window, verbose=0)
            predictions.append(pred[0][0])
            current_window = np.append(current_window[:, 1:, :], pred.reshape(1, 1, 1), axis=1)
        
        # Inverse transform predictions
        forecast_prices = scaler.inverse_transform(np.array(predictions).reshape(-1, 1)).flatten()
        
        # Aggregate hourly predictions to daily (mean)
        forecast_prices_daily = []
        forecast_dates = []
        for i in range(forecast_steps):
            day_start_idx = i * 24
            day_end_idx = (i + 1) * 24
            daily_price = np.mean(forecast_prices[day_start_idx:day_end_idx])
            forecast_prices_daily.append(float(daily_price))
            forecast_dates.append((current_date + timedelta(days=days_to_start + i)).strftime("%Y-%m-%d"))
        
        logger.info(f"Forecasted data points: {len(forecast_prices_daily)}")
        
        # Validate forecast variability
        forecast_std = np.std(forecast_prices_daily)
        logger.info(f"Forecast standard deviation: {forecast_std}")
        if forecast_std < 1e-5:
            logger.warning("Forecasted prices show very low variability.")
        
        # Calculate forecast summary metrics
        forecast_array = np.array(forecast_prices_daily)
        last_historical_price = historical_prices[-1]
        percent_change = ((forecast_array[-1] - last_historical_price) / last_historical_price * 100) if last_historical_price != 0 else 0.0
        forecast_stats = {
            "min": float(forecast_array.min()),
            "max": float(forecast_array.max()),
            "mean": float(forecast_array.mean()),
            "percent_change": float(percent_change)
        }
        logger.info(f"Forecast stats: {forecast_stats}")
        
        # Calculate sentiment probabilities based on percent_change
        # Use a logistic-like function to map percent_change to probabilities
        # We'll assume a neutral sentiment is most likely around 0% change
        # and bullish/bearish probabilities increase as percent_change moves away from 0
        def calculate_sentiment_probabilities(percent_change):
            # Scale factor to control how quickly probabilities change
            scale = 0.2  # Adjust this to make the transition sharper or softer
            # Logistic function for bullish probability
            bullish_prob = 1 / (1 + np.exp(-scale * (percent_change - 2)))  # Shifts center to +2%
            # Logistic function for bearish probability
            bearish_prob = 1 / (1 + np.exp(scale * (percent_change + 2)))  # Shifts center to -2%
            # Neutral probability is the remainder
            neutral_prob = 1 - bullish_prob - bearish_prob
            # Normalize to ensure they sum to 1 (in case of numerical issues)
            total = bullish_prob + bearish_prob + neutral_prob
            if total > 0:  # Avoid division by zero
                bullish_prob = bullish_prob / total * 100
                bearish_prob = bearish_prob / total * 100
                neutral_prob = neutral_prob / total * 100
            else:
                bullish_prob = bearish_prob = neutral_prob = 33.33  # Fallback to equal distribution
            # Ensure non-negative values
            bullish_prob = max(0, bullish_prob)
            bearish_prob = max(0, bearish_prob)
            neutral_prob = max(0, neutral_prob)
            # Re-normalize to 100%
            total = bullish_prob + bearish_prob + neutral_prob
            if total > 0:
                bullish_prob = bullish_prob / total * 100
                bearish_prob = bearish_prob / total * 100
                neutral_prob = neutral_prob / total * 100
            return {
                "bullish": round(bullish_prob, 2),
                "bearish": round(bearish_prob, 2),
                "neutral": round(neutral_prob, 2)
            }

        sentiment_probabilities = calculate_sentiment_probabilities(percent_change)
        logger.info(f"Sentiment probabilities: {sentiment_probabilities}")
        
        # Combine response
        response = {
            "historical": {
                "dates": historical_dates,
                "prices": historical_prices,
                "stats": hist_stats
            },
            "forecast": {
                "dates": forecast_dates,
                "prices": forecast_prices_daily,
                "stats": forecast_stats,
                "sentiment_probabilities": sentiment_probabilities
            }
        }
        
        return JSONResponse(content=response)
    
    except Exception as e:
        logger.error(f"Error in forecast endpoint: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)