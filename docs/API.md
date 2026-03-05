# 📡 API Reference

This document describes the internal Python APIs for each module in the Financial Data Pipeline.

---

## Ingestion Layer

### `StockDataProducer` (`src/ingestion/stock_producer.py`)

Main class for fetching and publishing stock data.

#### Methods

| Method | Parameters | Returns | Description |
|--------|-----------|---------|-------------|
| `fetch_stock_data_yfinance(symbol)` | `symbol: str` | `Dict \| None` | Fetch latest 1-min candle from Yahoo Finance |
| `fetch_stock_data_alpha_vantage(symbol)` | `symbol: str` | `Dict \| None` | Fetch quote from Alpha Vantage (requires API key) |
| `validate_data(data)` | `data: Dict` | `bool` | Validate required fields and value ranges |
| `publish_to_kafka(data)` | `data: Dict` | `bool` | Publish validated record to Kafka topic |
| `run_continuous(interval_seconds)` | `interval_seconds: int = 60` | `None` | Run in loop with configurable interval |
| `run_once()` | — | `None` | Fetch and publish a single batch (useful for testing) |

#### Stock Data Schema

```json
{
  "symbol": "AAPL",
  "timestamp": "2024-12-01T10:30:00",
  "open_price": 150.00,
  "high_price": 152.00,
  "low_price": 149.50,
  "close_price": 151.50,
  "volume": 1000000,
  "source": "yfinance",
  "data_quality": {
    "is_valid": true,
    "has_nulls": false
  }
}
```

---

### `CryptoDataProducer` (`src/ingestion/crypto_producer.py`)

Main class for fetching and publishing cryptocurrency data.

#### Methods

| Method | Parameters | Returns | Description |
|--------|-----------|---------|-------------|
| `fetch_crypto_data_coingecko(symbols)` | `symbols: List[str]` | `List[Dict]` | Fetch prices from CoinGecko (batch) |
| `validate_data(data)` | `data: Dict` | `bool` | Validate crypto record |
| `publish_to_kafka(data)` | `data: Dict` | `bool` | Publish to crypto Kafka topic |
| `run_continuous(interval_seconds)` | `interval_seconds: int = 60` | `None` | Run continuously |

#### Crypto Data Schema

```json
{
  "symbol": "bitcoin",
  "timestamp": "2024-12-01T10:30:00",
  "price_usd": 45000.00,
  "market_cap": 850000000000,
  "volume_24h": 30000000000,
  "price_change_24h": 2.5,
  "source": "coingecko"
}
```

---

## Storage Layer

### `DatabaseManager` (`src/storage/db_manager.py`)

Handles all PostgreSQL operations.

#### Methods

| Method | Parameters | Returns | Description |
|--------|-----------|---------|-------------|
| `get_latest_prices(symbols)` | `symbols: List[str] \| None` | `List[Dict]` | Get most recent price for each symbol |
| `get_historical_data(symbol, days)` | `symbol: str, days: int = 7` | `List[Dict]` | Get OHLCV history |
| `get_aggregated_metrics(symbol, window, limit)` | `symbol: str, window: str, limit: int` | `List[Dict]` | Get windowed metrics (`'1min'` or `'5min'`) |
| `get_anomalies(symbol, hours)` | `symbol: str \| None, hours: int = 24` | `List[Dict]` | Get detected anomalies |
| `insert_pipeline_run(...)` | See signature | `None` | Log a pipeline execution event |
| `get_data_quality_metrics()` | — | `Dict` | Return record counts, freshness, null counts |

#### Example Usage

```python
from src.storage.db_manager import DatabaseManager

db = DatabaseManager()

# Get latest prices for all symbols
prices = db.get_latest_prices()

# Get last 3 days of AAPL data
history = db.get_historical_data('AAPL', days=3)

# Get 5-minute aggregated metrics
metrics = db.get_aggregated_metrics('AAPL', window='5min', limit=50)

# Get data quality report
quality = db.get_data_quality_metrics()
print(quality)
# {
#   'total_stock_records': 5000,
#   'total_crypto_records': 2000,
#   'data_freshness_minutes': 1.5,
#   'records_with_nulls': 0
# }
```

---

### `CacheManager` (`src/storage/db_manager.py`)

Handles Redis caching operations.

#### Methods

| Method | Parameters | Returns | Description |
|--------|-----------|---------|-------------|
| `set_latest_price(symbol, price, ttl)` | `symbol: str, price: float, ttl: int = 300` | `None` | Cache price with TTL (seconds) |
| `get_latest_price(symbol)` | `symbol: str` | `float \| None` | Get cached price |
| `set_market_summary(data, ttl)` | `data: Dict, ttl: int = 60` | `None` | Cache market summary JSON |
| `get_market_summary()` | — | `Dict \| None` | Get cached market summary |

#### Redis Key Conventions

| Key Pattern | Value Type | TTL | Description |
|-------------|-----------|-----|-------------|
| `price:{SYMBOL}` | String (float) | 300s | Latest price for a symbol |
| `market:summary` | JSON string | 60s | Aggregated market overview |

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|---------|---------|-------------|
| `ALPHA_VANTAGE_API_KEY` | No | — | Alpha Vantage API key (fallback source) |
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | `localhost:9092` | Kafka broker address |
| `KAFKA_STOCK_TOPIC` | No | `stock-prices-raw` | Kafka topic for stock data |
| `KAFKA_CRYPTO_TOPIC` | No | `crypto-prices-raw` | Kafka topic for crypto data |
| `DB_HOST` | No | `localhost` | PostgreSQL host |
| `DB_PORT` | No | `5432` | PostgreSQL port |
| `DB_NAME` | No | `financial_data` | PostgreSQL database name |
| `DB_USER` | No | `dataeng` | PostgreSQL username |
| `DB_PASSWORD` | No | `dataeng123` | PostgreSQL password |
| `REDIS_HOST` | No | `localhost` | Redis host |
| `REDIS_PORT` | No | `6379` | Redis port |
| `STOCK_SYMBOLS` | No | `AAPL,GOOGL,MSFT` | Comma-separated stock symbols to track |
| `CRYPTO_SYMBOLS` | No | `bitcoin,ethereum` | Comma-separated crypto IDs to track |
| `INGESTION_INTERVAL_SECONDS` | No | `60` | How often producers fetch data |
