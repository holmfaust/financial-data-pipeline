# Multi-stage build for Financial Data Pipeline
FROM python:3.9-slim AS base

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY config/ ./config/
COPY migrations/ ./migrations/
COPY .env.example .env

# Default environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DB_HOST=postgres \
    DB_PORT=5432 \
    DB_NAME=financial_data \
    DB_USER=dataeng \
    DB_PASSWORD=dataeng123 \
    REDIS_HOST=redis \
    REDIS_PORT=6379 \
    KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
    KAFKA_STOCK_TOPIC=stock-prices-raw \
    KAFKA_CRYPTO_TOPIC=crypto-prices-raw \
    STOCK_SYMBOLS=AAPL,GOOGL,MSFT,AMZN,TSLA \
    CRYPTO_SYMBOLS=bitcoin,ethereum \
    INGESTION_INTERVAL_SECONDS=60

# Expose Streamlit dashboard port
EXPOSE 8501

# Default command: run the stock producer
# Override with docker run --entrypoint or docker-compose command
CMD ["python", "src/ingestion/stock_producer.py"]
