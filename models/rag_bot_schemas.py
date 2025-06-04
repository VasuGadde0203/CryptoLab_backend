from pydantic import BaseModel

# Request schema
class QueryRequest(BaseModel):
    query: str