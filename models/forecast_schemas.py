from pydantic import BaseModel, EmailStr
from typing import List, Dict

class forecast_request(BaseModel):
    start_date: str 
    end_date: str