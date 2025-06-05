# CryptoLab Backend

## **Overview**
CryptoLab Backend is a FastAPI-based API server designed to power the CryptoLab application, a cryptocurrency price forecasting tool. It integrates with the Binance API for data fetching, uses an LSTM model for price forecasting, and handles user queries via email using SMTP. As of June 05, 2025, the backend is in development and has not yet been deployed.
Features

- **Price Forecasting:** Uses an LSTM model to predict Bitcoin prices based on historical data fetched from Binance.
- **Market Sentiment Analysis:** Analyzes market trends and provides sentiment probabilities (bullish, bearish, neutral).
- **Data Fetching:** Retrieves real-time and historical cryptocurrency data via the Binance API.
- **Contact Form Handling:** Processes user queries from the frontend and sends them as emails using SMTP (Gmail).
- **Authentication:** Implements JWT-based authentication for secure API access (used for forecasting endpoints).

## Tech Stack

- **Framework:** FastAPI
- **Language:** Python 3.11
- **Dependencies:**
    - **python-binance:** For interacting with Binance APIs.
    - **tensorflow:** For the LSTM model used in forecasting.
    - **pandas, numpy:** For data manipulation.
    - **smtplib:** For sending emails via Gmail SMTP.
    - **python-dotenv:** For managing environment variables.
    - **PyJWT:** For JWT authentication.
    - See requirements.txt for the full list.


## **Server:** Planned to use Gunicorn + Uvicorn (production WSGI/ASGI server)
## **Reverse Proxy:** Planned to use Nginx

## **Prerequisites**
- Python 3.11
- Git
- Binance API key and secret (for data fetching)
- Gmail account with App Password (for SMTP)

## **Setup Instructions (Local Development)**

- **Clone the Repository:**
    - git clone https://github.com/your-username/cryptolab-backend.git
    - cd cryptolab-backend

- **Set Up a Virtual Environment:**
    - python3.11 -m venv venv
    - source venv/bin/activate  # On Windows: venv\Scripts\activate

- **Install Dependencies:**
    - pip install --upgrade pip
    - pip install -r requirements.txt

- **Configure Environment Variables:**
    - Copy the example .env file:cp .env.example .env

- **Edit .env with your credentials:**
  - MONGO_URI = "your-mongo-uri"
  - JWT_SECRET = "your-jwt-secret"
  - AZURE_OPENAI_API_KEY = "your-azure-openai-key"
  - AZURE_OPENAI_ENDPOINT = "your-azure-openai-endpoint"
  - PINECONE_API_KEY = "your-pinecone-api-key"
  - SENDER_EMAIL = "your-email"
  - SENDER_PASSWORD = "your-app-password"

  - Note: Use a Gmail App Password (not your regular password). Generate one via Google Account settings > Security > 2-Step Verification > App Passwords.


## **Run the Application:**
- uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
- Access the API at http://localhost:8000/docs to view the Swagger UI.


## API Endpoints

- **GET /:** Health check endpoint ({"message": "CryptoLab Backend is running"}).
- **POST /api/forecast:** Fetches historical data, runs LSTM predictions, and returns forecasted prices and sentiment analysis.
- Requires JWT authentication.
- Request body example:{
    "start_date": "2025-06-05",
    "end_date": "2025-06-11"
  }

- POST /contact/query: Processes user queries and sends them as emails.
= Request body example:{
    "name": "John Doe",
    "email": "john@example.com",
    "query": "I have a question about Bitcoin forecasting."
  }


## **Future Plans**

- **Deployment:** Deploy the backend to AWS EC2 using the Free Tier (t2.micro instance).
  - Set up Nginx as a reverse proxy.
  - Use Gunicorn + Uvicorn for production.
  - Configure a systemd service for automatic restarts.

- **HTTPS Support:** Add SSL/TLS using AWS Certificate Manager or Cloudflare.
- **Rate Limiting:** Implement rate limiting to prevent abuse (e.g., using slowapi).
- **Monitoring:** Set up logging and monitoring using AWS CloudWatch.

## Contributing

- Fork the repository.
- Create a feature branch (git checkout -b feature-name).
- Commit your changes (git commit -m "Add feature").
- Push to the branch (git push origin feature-name).
- Open a Pull Request.

## License
- This project is licensed under the MIT License.

## Contact
- For inquiries, email vasugadde0203@gmail.com.
