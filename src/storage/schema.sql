-- Financial Data Pipeline Database Schema

-- Raw Stock Prices
CREATE TABLE IF NOT EXISTS stock_prices_raw (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open_price DECIMAL(12, 4),
    high_price DECIMAL(12, 4),
    low_price DECIMAL(12, 4),
    close_price DECIMAL(12, 4),
    volume BIGINT,
    source VARCHAR(50),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp, source)
);

-- Raw Crypto Prices
CREATE TABLE IF NOT EXISTS crypto_prices_raw (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    price_usd DECIMAL(18, 8),
    market_cap DECIMAL(20, 2),
    volume_24h DECIMAL(20, 2),
    percent_change_24h DECIMAL(8, 4),
    source VARCHAR(50),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp, source)
);

-- Processed Stock Metrics (1-minute aggregations)
CREATE TABLE IF NOT EXISTS stock_metrics_1min (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    open_price DECIMAL(12, 4),
    high_price DECIMAL(12, 4),
    low_price DECIMAL(12, 4),
    close_price DECIMAL(12, 4),
    avg_price DECIMAL(12, 4),
    volume BIGINT,
    price_change DECIMAL(12, 4),
    price_change_pct DECIMAL(8, 4),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, window_start)
);

-- Processed Stock Metrics (5-minute aggregations)
CREATE TABLE IF NOT EXISTS stock_metrics_5min (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    open_price DECIMAL(12, 4),
    high_price DECIMAL(12, 4),
    low_price DECIMAL(12, 4),
    close_price DECIMAL(12, 4),
    avg_price DECIMAL(12, 4),
    volume BIGINT,
    volatility DECIMAL(12, 6),
    sma_20 DECIMAL(12, 4),
    ema_20 DECIMAL(12, 4),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, window_start)
);

-- Technical Indicators
CREATE TABLE IF NOT EXISTS technical_indicators (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    indicator_type VARCHAR(50) NOT NULL,
    indicator_value DECIMAL(12, 4),
    upper_band DECIMAL(12, 4),
    lower_band DECIMAL(12, 4),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp, indicator_type)
);

-- Anomaly Detection Results
CREATE TABLE IF NOT EXISTS price_anomalies (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    anomaly_type VARCHAR(50),
    expected_price DECIMAL(12, 4),
    actual_price DECIMAL(12, 4),
    deviation_pct DECIMAL(8, 4),
    severity VARCHAR(20),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Quality Metrics
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(12, 4),
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline Monitoring
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    status VARCHAR(50),
    records_processed INTEGER,
    records_failed INTEGER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    error_message TEXT
);

-- Indexes for Performance
CREATE INDEX idx_stock_prices_symbol_timestamp ON stock_prices_raw(symbol, timestamp DESC);
CREATE INDEX idx_crypto_prices_symbol_timestamp ON crypto_prices_raw(symbol, timestamp DESC);
CREATE INDEX idx_stock_metrics_1min_symbol_window ON stock_metrics_1min(symbol, window_start DESC);
CREATE INDEX idx_stock_metrics_5min_symbol_window ON stock_metrics_5min(symbol, window_start DESC);
CREATE INDEX idx_anomalies_symbol_timestamp ON price_anomalies(symbol, timestamp DESC);

-- Views for Easy Querying
CREATE OR REPLACE VIEW latest_stock_prices AS
SELECT DISTINCT ON (symbol)
    symbol,
    close_price as current_price,
    volume,
    timestamp,
    ((close_price - LAG(close_price) OVER (PARTITION BY symbol ORDER BY timestamp)) / 
     LAG(close_price) OVER (PARTITION BY symbol ORDER BY timestamp) * 100) as change_pct
FROM stock_prices_raw
ORDER BY symbol, timestamp DESC;

CREATE OR REPLACE VIEW daily_stock_summary AS
SELECT 
    symbol,
    DATE(timestamp) as trade_date,
    MIN(low_price) as day_low,
    MAX(high_price) as day_high,
    FIRST_VALUE(open_price) OVER (PARTITION BY symbol, DATE(timestamp) ORDER BY timestamp) as day_open,
    LAST_VALUE(close_price) OVER (PARTITION BY symbol, DATE(timestamp) ORDER BY timestamp 
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as day_close,
    SUM(volume) as total_volume
FROM stock_prices_raw
GROUP BY symbol, DATE(timestamp), open_price, close_price, timestamp;

-- Grant Permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dataeng;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dataeng;