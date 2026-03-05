# 🚀 Real-time Financial Data Pipeline

A production-grade, scalable real-time data pipeline for ingesting, processing, and visualizing financial market data. This project demonstrates end-to-end data engineering skills including streaming data ingestion, ETL processes, data warehousing, and real-time analytics.

![Python](https://img.shields.io/badge/Python-3.9+-blue)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Latest-black)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-Latest-blue)

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Monitoring](#monitoring)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Future Enhancements](#future-enhancements)

## 🏗️ Architecture Overview

```
┌─────────────────┐
│  Data Sources   │
│  - Alpha Vantage│
│  - Yahoo Finance│
│  - CoinGecko    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Kafka Broker   │  ◄── Message Queue
│  (Pub/Sub)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Spark Streaming │  ◄── Stream Processing
│  - Aggregations │      (Windowing, Metrics)
│  - Technical    │
│    Indicators   │
│  - Anomaly      │
│    Detection    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌──────────────┐
│   PostgreSQL    │────►│    Redis     │
│ (Data Warehouse)│     │   (Cache)    │
└────────┬────────┘     └──────────────┘
         │
         ▼
┌─────────────────┐
│   Streamlit     │  ◄── Visualization
│   Dashboard     │      (Real-time UI)
└─────────────────┘
```

## ✨ Features

### 1. **Multi-Source Data Ingestion**
- Real-time stock prices from Yahoo Finance & Alpha Vantage
- Cryptocurrency prices from CoinGecko & Binance
- Fault-tolerant ingestion with automatic failover
- Rate limiting and API quota management

### 2. **Stream Processing**
- Real-time window aggregations (1-min, 5-min)
- Technical indicators:
  - Simple Moving Average (SMA)
  - Exponential Moving Average (EMA)
  - Bollinger Bands
  - Volatility calculations
- Statistical anomaly detection (3-sigma rule)
- Late data handling with watermarks

### 3. **Data Storage**
- **Hot Storage**: Redis for latest prices and caching
- **Warm Storage**: PostgreSQL for recent data and fast queries
- **Schema Evolution**: Versioned database migrations
- Optimized indexes for query performance

### 4. **Data Quality**
- Schema validation
- Duplicate detection
- Missing data handling
- Data freshness monitoring
- Automated quality metrics

### 5. **Visualization**
- Real-time price updates
- Interactive candlestick charts
- Volume analysis
- Technical indicator plots
- Anomaly alerts

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Data Ingestion** | Apache Kafka | Message broker for streaming data |
| **Stream Processing** | Apache Spark Streaming | Real-time data transformation |
| **Data Storage** | PostgreSQL | Primary data warehouse |
| **Caching** | Redis | Fast in-memory cache |
| **Containerization** | Docker, Docker Compose | Service orchestration |
| **Visualization** | Streamlit, Plotly | Interactive dashboards |
| **Testing** | Pytest | Unit and integration tests |
| **Programming** | Python 3.9+ | Primary language |

## 📦 Prerequisites

### Required Software
- Docker Desktop (latest version)
- Python 3.9 or higher
- Git

### API Keys (Free Tier)
1. **Alpha Vantage**: https://www.alphavantage.co/support/#api-key
2. **CoinGecko**: No API key required for free tier

## 🚀 Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/financial-data-pipeline.git
cd financial-data-pipeline
```

### Step 2: Environment Setup

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Configure Environment Variables

```bash
# Copy the example .env file
cp .env.example .env

# Edit .env and add your API keys
# ALPHA_VANTAGE_API_KEY=your_key_here
```

### Step 4: Start Infrastructure

```bash
# Start all services (Kafka, Postgres, Redis, Spark)
docker-compose up -d

# Verify all services are running
docker-compose ps
```

Expected output:
```
NAME            COMMAND                  SERVICE      STATUS
kafka           "/etc/confluent/..."    kafka        running
postgres-db     "docker-entrypoint..."  postgres     running
redis-cache     "docker-entrypoint..."  redis        running
spark-master    "/opt/bitnami/..."      spark-master running
```

### Step 5: Initialize Database

```bash
# Database schema is automatically initialized via docker-compose
# Verify tables exist
docker exec -it postgres-db psql -U dataeng -d financial_data -c "\dt"
```

## 📊 Usage

### Running the Complete Pipeline

#### Terminal 1: Start Stock Producer
```bash
python src/ingestion/stock_producer.py
```

#### Terminal 2: Start Crypto Producer
```bash
python src/ingestion/crypto_producer.py
```

#### Terminal 3: Start Spark Streaming
```bash
python src/processing/spark_streaming.py
```

#### Terminal 4: Launch Dashboard
```bash
streamlit run src/visualization/dashboard.py
```

Access the dashboard at: http://localhost:8501

### Running Individual Components

#### Test Data Ingestion (Single Batch)
```bash
python -c "from src.ingestion.stock_producer import StockDataProducer; StockDataProducer().run_once()"
```

#### Check Kafka Topics
```bash
# Access Kafka UI
open http://localhost:8090
```

#### Query Database
```bash
docker exec -it postgres-db psql -U dataeng -d financial_data

# Example queries
SELECT * FROM stock_prices_raw ORDER BY timestamp DESC LIMIT 10;
SELECT * FROM price_anomalies;
```

#### Check Redis Cache
```bash
docker exec -it redis-cache redis-cli

# Example commands
> KEYS *
> GET price:AAPL
```

## 📁 Project Structure

```
financial-data-pipeline/
├── docker-compose.yml           # Infrastructure orchestration
├── requirements.txt             # Python dependencies
├── .env                         # Environment variables
├── README.md                    # This file
│
├── config/                      # Configuration files
│   ├── kafka_config.yaml
│   └── spark_config.yaml
│
├── src/
│   ├── ingestion/              # Data ingestion layer
│   │   ├── stock_producer.py   # Stock data producer
│   │   └── crypto_producer.py  # Crypto data producer
│   │
│   ├── processing/             # Stream processing
│   │   ├── spark_streaming.py  # Spark streaming job
│   │   └── data_transformer.py # Data transformations
│   │
│   ├── storage/                # Data storage layer
│   │   ├── db_manager.py       # Database operations
│   │   └── schema.sql          # Database schema
│   │
│   └── visualization/          # Visualization layer
│       └── dashboard.py        # Streamlit dashboard
│
├── tests/                      # Test suite
│   ├── test_ingestion.py
│   ├── test_processing.py
│   └── test_storage.py
│
└── docs/                       # Additional documentation
    ├── ARCHITECTURE.md
    └── API.md
```

> 📖 See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for full system design and [API.md](docs/API.md) for module reference.

## 🔄 Data Flow

### 1. **Ingestion Phase**
```
APIs → Producers → Kafka Topics
```
- Producers fetch data every 60 seconds (configurable)
- Data validated before publishing to Kafka
- Failed requests automatically retry with backoff

### 2. **Processing Phase**
```
Kafka → Spark Streaming → Transformations → Database
```
- Spark reads micro-batches from Kafka
- Applies windowed aggregations
- Calculates technical indicators
- Detects anomalies
- Writes to PostgreSQL

### 3. **Storage Phase**
```
Spark → PostgreSQL (Warehouse) + Redis (Cache)
```
- Raw data stored in fact tables
- Aggregated metrics in dimension tables
- Latest prices cached in Redis

### 4. **Visualization Phase**
```
Database → Streamlit → User Interface
```
- Dashboard queries database
- Auto-refresh every 10 seconds
- Interactive charts and metrics

## 📈 Monitoring

### Kafka UI
Access at http://localhost:8090
- View topics and messages
- Monitor consumer lag
- Check partition distribution

### Spark UI
Access at http://localhost:8080
- Monitor streaming jobs
- View job statistics
- Check resource usage

### Database Metrics
```python
from src.storage.db_manager import DatabaseManager

db = DatabaseManager()
metrics = db.get_data_quality_metrics()
print(metrics)
```

### Logs
```bash
# View logs for specific service
docker-compose logs -f kafka
docker-compose logs -f spark-master
docker-compose logs -f postgres
```

## 🧪 Testing

### Run All Tests
```bash
pytest tests/ -v
```

### Run Specific Test Suite
```bash
# Test ingestion
pytest tests/test_ingestion.py -v

# Test with coverage
pytest tests/ --cov=src --cov-report=html
```

### Manual Testing
```bash
# Test database connection
python -c "from src.storage.db_manager import DatabaseManager; DatabaseManager().get_data_quality_metrics()"

# Test Redis cache
python -c "from src.storage.db_manager import CacheManager; CacheManager().set_latest_price('TEST', 100.0)"
```

## 🐛 Troubleshooting

### Common Issues

#### 1. Kafka Connection Error
```bash
# Check if Kafka is running
docker-compose ps kafka

# Restart Kafka
docker-compose restart kafka
```

#### 2. Database Connection Error
```bash
# Verify database is running
docker exec -it postgres-db pg_isready

# Check connection
docker exec -it postgres-db psql -U dataeng -d financial_data -c "SELECT 1;"
```

#### 3. API Rate Limits
- Alpha Vantage: 5 calls/minute (free tier)
- Solution: Increase `INGESTION_INTERVAL_SECONDS` in `.env`

#### 4. Spark Out of Memory
```yaml
# Increase memory in docker-compose.yml
spark-worker:
  environment:
    - SPARK_WORKER_MEMORY=4G
```

#### 5. Port Already in Use
```bash
# Find process using port
lsof -i :9092  # Kafka
lsof -i :5432  # PostgreSQL

# Kill process or change port in docker-compose.yml
```

## 🚀 Future Enhancements

### Short-term
- [ ] Add Apache Airflow for orchestration
- [ ] Implement data lineage tracking
- [ ] Add alerting system (email/Slack)
- [ ] Create API endpoint for data access

### Medium-term
- [ ] Migrate to cloud (AWS/GCP)
- [ ] Implement data partitioning strategy
- [ ] Add machine learning predictions
- [ ] Create mobile dashboard

### Long-term
- [ ] Implement multi-region deployment
- [ ] Add blockchain data sources
- [ ] Build recommendation engine
- [ ] Create data marketplace

## 📝 Key Learnings for Interview

This project demonstrates:

1. **System Design**: Designed a scalable, fault-tolerant architecture
2. **Stream Processing**: Handled real-time data with low latency
3. **Data Quality**: Implemented validation and monitoring
4. **Performance**: Optimized for throughput and low latency
5. **DevOps**: Containerized services for easy deployment
6. **Testing**: Unit tests with good coverage
7. **Documentation**: Clear, comprehensive docs

## 🤝 Contributing

Feel free to fork, improve, and create pull requests!

## 📄 License

MIT License

---

**Built for**: Data Engineer Position at LSEG  
**Date**: December 2024