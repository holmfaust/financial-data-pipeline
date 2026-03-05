# 🏗️ Architecture Overview

## System Design

The Financial Data Pipeline follows a **Lambda Architecture** pattern with a real-time streaming layer:

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   Yahoo Finance API    Alpha Vantage API    CoinGecko API        │
└───────────────┬─────────────────┬──────────────────┬────────────┘
                │                 │                  │
                ▼                 ▼                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                               │
│       stock_producer.py          crypto_producer.py              │
│  - Fault-tolerant fetching    - Multi-source with fallback       │
│  - Data validation            - Rate limit handling              │
└──────────────────────────────┬──────────────────────────────────┘
                               │ Kafka Topics:
                               │   stock-prices-raw
                               │   crypto-prices-raw
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                  STREAM PROCESSING LAYER                         │
│                    spark_streaming.py                            │
│  - 1-min window aggregations    - Watermark: 5 min late data     │
│  - 5-min window aggregations    - Anomaly detection (5% swing)   │
│  - Write raw + metrics to DB    - Volatility calculations        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
               ┌───────────────┴───────────────┐
               ▼                               ▼
┌──────────────────────────┐    ┌──────────────────────────────┐
│      PostgreSQL 15        │    │          Redis 7              │
│  (Primary Data Warehouse) │    │       (Hot Cache)             │
│  - stock_prices_raw       │    │  - price:{SYMBOL} (TTL 5min) │
│  - crypto_prices_raw      │    │  - market:summary (TTL 1min) │
│  - stock_metrics_1min     │    └──────────────────────────────┘
│  - stock_metrics_5min     │
│  - price_anomalies        │
│  - pipeline_runs          │
└──────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   VISUALIZATION LAYER                            │
│                      dashboard.py (Streamlit)                    │
│  - Real-time candlestick charts     - Auto-refresh 10s           │
│  - Volume analysis                  - Anomaly alerts             │
│  - Technical indicator plots        - Multi-symbol support       │
└─────────────────────────────────────────────────────────────────┘
```

## Component Details

### Ingestion Layer

| Component | File | Responsibilities |
|-----------|------|-----------------|
| StockDataProducer | `src/ingestion/stock_producer.py` | Fetch stock data from Yahoo Finance (primary) and Alpha Vantage (fallback), validate, publish to Kafka |
| CryptoDataProducer | `src/ingestion/crypto_producer.py` | Fetch crypto prices from CoinGecko and Binance, validate, publish to Kafka |

**Fault Tolerance:**
- Automatic failover from Yahoo Finance → Alpha Vantage for stocks
- CoinGecko → Binance for crypto
- Exponential backoff on API failures
- Data validation before publishing

### Stream Processing Layer

Implemented using **Apache Spark Structured Streaming**:

- **1-minute windows**: OHLC aggregation, volume sum, tick count
- **5-minute windows**: Volatility (stddev), aggregated OHLC
- **Anomaly detection**: Flags symbols with >5% price swing within a window
- **Watermarks**: 5-minute tolerance for late-arriving data

> **Note**: Row-based moving averages (SMA/EMA) are not supported natively in structured streaming. These require `mapGroupsWithState` or a batch layer, and are set as `null` placeholders currently.

### Storage Layer

#### PostgreSQL Schema

```
stock_prices_raw          → Raw tick data from producers
crypto_prices_raw         → Raw crypto tick data
stock_metrics_1min        → 1-minute OHLC aggregations
stock_metrics_5min        → 5-minute OHLC + volatility
price_anomalies           → Detected price anomaly events
pipeline_runs             → Pipeline execution audit log
```

#### Redis Cache

```
price:{SYMBOL}            → Latest price float (TTL: 5 min)
market:summary            → JSON market overview (TTL: 1 min)
```

### Monitoring Layer

`src/monitoring/data_lineage.py` tracks:
- Data flow from source → Kafka → Spark → DB
- Record counts at each stage
- Pipeline run audit log

## Design Decisions

1. **Yahoo Finance as primary source**: Free, no API key needed, good reliability
2. **Kafka over direct DB writes**: Decouples producers from consumers, enables replay
3. **Spark Streaming over Flink**: Better Python support, easier local development
4. **PostgreSQL over Cassandra**: Simpler ops for a portfolio project, good for analytical queries
5. **Redis TTL strategy**: Short TTL (5 min) to avoid stale prices, long enough to reduce DB load
