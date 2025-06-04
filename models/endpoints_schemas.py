from pydantic import BaseModel


## Spot Account Information - No start time and endtime
## Spot trades list - symbol, starttime, endtime


# Pydantic model for request body
class SpotAccountRequest(BaseModel):
    account_name: str
    client_name: str
    
# Pydantic model for request body
class TradeListRequest(BaseModel):
    client_name: str
    account_name: str
    symbol: str | None = None  # Optional: If not provided, fetch for all symbols
    start_time: int | None = None  # Optional: Unix timestamp in milliseconds
    end_time: int | None = None  # Optional: Unix timestamp in milliseconds
    limit: int = 500  # Default: 500, max: 1000

# Pydantic model for request body
class UniversalTransferRequest(BaseModel):
    client_name: str
    account_name: str
    start_time: int | None = None  # Optional: Unix timestamp in milliseconds
    end_time: int | None = None  # Optional: Unix timestamp in milliseconds
    
# Pydantic model for request body
class FuturesAccountRequest(BaseModel):
    client_name: str
    account_name: str
    
# Pydantic model for request body
class FuturesTradeListRequest(BaseModel):
    client_name: str 
    account_name: str 
    symbol: str | None = None  # Optional: If not provided, fetch for all symbols
    start_time: int | None = None  # Optional: Unix timestamp in milliseconds
    end_time: int | None = None  # Optional: Unix timestamp in milliseconds
    limit: int = 500  # Default: 500, max: 1000
    
# Pydantic model for request body
class FuturesPositionInfoRequest(BaseModel):
    client_name: str 
    account_name: str
    
# Pydantic model for request body
class FuturesAccountBalancesRequest(BaseModel):
    client_name: str
    account_name: str