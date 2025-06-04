from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import smtplib
from email.mime.text import MIMEText
import os


contact_router = APIRouter()

class ContactQuery(BaseModel):
    name: str
    email: str
    query: str

@contact_router.post("/contact/query")
async def send_query(query_request: ContactQuery):
    try:
        # Retrieve email settings from environment variables
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        sender_email = "querycryptolab@gmail.com"
        sender_password = "hrrj sugo zzkn kmxl"
        receiver_email = "vasugadde0203@gmail.com"

        # Validate environment variables
        if not sender_email or not sender_password:
            raise HTTPException(status_code=500, detail="Email configuration is missing. Please set SENDER_EMAIL and SENDER_PASSWORD in the .env file.")

        # Create email message
        subject = f"New Query from {query_request.name}"
        body = f"Name: {query_request.name}\nEmail: {query_request.email}\nQuery: {query_request.query}"
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = sender_email
        msg["To"] = receiver_email

        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())

        return {"message": "Query submitted successfully"}
    except smtplib.SMTPAuthenticationError:
        raise HTTPException(status_code=500, detail="Failed to authenticate with the email server. Please check your SENDER_EMAIL and SENDER_PASSWORD.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send email: {str(e)}")