from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Dict
from langchain_community.vectorstores import FAISS
from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings
# from langchain.embeddings import AzureOpenAIEmbeddings
from langchain.schema import HumanMessage
import os
from models.rag_bot_schemas import *
from database.auth import *
from database.mongo_ops import *
from bson import ObjectId

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

rag_bot_router = APIRouter()

# Load FAISS index
faiss_index = FAISS.load_local("binance_docs_index_store", embeddings=AzureOpenAIEmbeddings(
    azure_deployment='embedding_model',
    openai_api_version='2023-05-15',
    chunk_size=1000
), allow_dangerous_deserialization=True)

# Azure OpenAI GPT setup
chat = AzureChatOpenAI(
    openai_api_version="2024-05-01-preview",
    azure_deployment="model-4o",
    temperature=0.7
)

# API endpoint
@rag_bot_router.post("/chat/query")
def handle_query(request: QueryRequest, user: dict = Depends(get_current_user)):
    # Step 1: Get user_id
    print("In rag bot")
    user_id = user["user_id"]
    user_message = {
        "role": "user",
        "text": request.query,
        "timestamp": datetime.utcnow()
    }
    # Step 2: Search similar documents
    relevant_docs = faiss_index.similarity_search(request.query, k=4)
    if not relevant_docs:
        return {"response": "No relevant information found."}

    # Step 3: Prepare prompt
    context = "\n\n".join([doc.page_content for doc in relevant_docs])
    prompt = f"""Use the following extracted document content to answer the question:

        {context}

        Question: {request.query}

        Answer:"""
        

    # Step 4: Get answer from Azure GPT
    response = chat([HumanMessage(content=prompt)])
    
    bot_message = {
        "role": "bot",
        "text": response.content,
        "timestamp": datetime.utcnow()
    }
    
    # Check for existing conversation
    conversation = conversations_collection.find_one({"user_id": user_id})

    if conversation:
        conversations_collection.update_one(
            {"user_id": conversation["user_id"]},
            {"$push": {"messages": {"$each": [user_message, bot_message]}}}
        )
    else:
        conversations_collection.insert_one({
            "user_id": user_id,
            "messages": [user_message, bot_message]
        })

    # Step 6: Return answer
    return {"answer": response.content}