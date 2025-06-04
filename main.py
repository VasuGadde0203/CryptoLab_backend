from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes.auth_routes import *
from routes.binance_routes import *
from routes.timeseries_routes import *
from routes.rag_bot_routes import *
from routes.forecasting_routes import *
from routes.contact_routes import *

app = FastAPI()

app.include_router(auth_router)
app.include_router(binance_router)
app.include_router(timeseries_router)
app.include_router(rag_bot_router)
app.include_router(forecast_router)
app.include_router(contact_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, set this to your frontend domain instead of "*"
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"message": "Welcome to the AI-Powered News Recommender API"}
