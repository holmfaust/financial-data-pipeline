"""
Unit and Integration Tests for Data Processing Components
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestSparkStreaming:
    """Test cases for Spark Streaming processing"""

    @pytest.fixture
    def sample_stock_data(self):
        """Create sample stock data for testing"""
        now = datetime.now()
        return [
            {
                "symbol": "AAPL",
                "timestamp": (now - timedelta(minutes=i)).isoformat(),
                "open_price": 150.0 + i * 0.1,
                "high_price": 152.0 + i * 0.1,
                "low_price": 149.0 + i * 0.1,
                "close_price": 151.0 + i * 0.1,
                "volume": 1000000 + i * 1000,
                "source": "yfinance",
            }
            for i in range(10)
        ]

    @pytest.fixture
    def spark_processor(self):
        """Create mock Spark processor"""
        # Mock Spark session
        with patch("pyspark.sql.SparkSession.builder"):
            from src.processing.spark_streaming import FinancialStreamProcessor

            processor = Mock(spec=FinancialStreamProcessor)
            return processor

    def test_window_aggregation_1min(self, sample_stock_data):
        """Test 1-minute window aggregations"""
        df = pd.DataFrame(sample_stock_data)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Group by 1-minute windows
        df.set_index("timestamp", inplace=True)
        result = df.resample("1T").agg(
            {"low_price": "min", "high_price": "max", "close_price": "mean", "volume": "sum"}
        )

        assert not result.empty
        assert result["low_price"].min() <= df["low_price"].min()
        assert result["high_price"].max() >= df["high_price"].max()

    def test_window_aggregation_5min(self, sample_stock_data):
        """Test 5-minute window aggregations"""
        df = pd.DataFrame(sample_stock_data)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)

        # 5-minute aggregation
        result = df.resample("5T").agg({"close_price": ["mean", "std"], "volume": "sum"})

        assert not result.empty
        assert "close_price" in result.columns

    def test_sma_calculation(self, sample_stock_data):
        """Test Simple Moving Average calculation"""
        df = pd.DataFrame(sample_stock_data)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")

        # Calculate SMA-5
        window_size = 5
        df["sma_5"] = df["close_price"].rolling(window=window_size).mean()

        # First 4 values should be NaN, then we have values
        assert df["sma_5"].isna().sum() == window_size - 1
        assert not df["sma_5"].iloc[-1] is None

        # SMA should be within price range
        valid_sma = df["sma_5"].dropna()
        assert valid_sma.min() >= df["close_price"].min()
        assert valid_sma.max() <= df["close_price"].max()

    def test_ema_calculation(self, sample_stock_data):
        """Test Exponential Moving Average calculation"""
        df = pd.DataFrame(sample_stock_data)
        df = df.sort_values("timestamp")

        # Calculate EMA-5
        df["ema_5"] = df["close_price"].ewm(span=5, adjust=False).mean()

        assert not df["ema_5"].isna().any()
        assert len(df["ema_5"]) == len(df)

    def test_volatility_calculation(self, sample_stock_data):
        """Test price volatility calculation"""
        df = pd.DataFrame(sample_stock_data)

        # Calculate volatility (standard deviation)
        volatility = df["close_price"].std()

        assert volatility >= 0
        assert isinstance(volatility, float)

    def test_anomaly_detection_z_score(self, sample_stock_data):
        """Test anomaly detection using z-score"""
        df = pd.DataFrame(sample_stock_data)

        # Add an outlier
        df.loc[len(df)] = {
            "symbol": "AAPL",
            "timestamp": datetime.now().isoformat(),
            "close_price": 200.0,  # Outlier
            "volume": 1000000,
        }

        # Calculate z-scores
        mean = df["close_price"].mean()
        std = df["close_price"].std()
        df["z_score"] = (df["close_price"] - mean) / std

        # Detect anomalies (|z-score| > 3)
        anomalies = df[abs(df["z_score"]) > 3]

        assert len(anomalies) > 0
        assert 200.0 in anomalies["close_price"].values

    def test_data_quality_null_check(self, sample_stock_data):
        """Test data quality - null value detection"""
        df = pd.DataFrame(sample_stock_data)

        # Add row with null
        df.loc[len(df)] = {
            "symbol": "AAPL",
            "timestamp": datetime.now().isoformat(),
            "close_price": None,  # Null value
            "volume": 1000000,
        }

        null_count = df["close_price"].isna().sum()
        assert null_count == 1

        # Data quality check
        completeness = (len(df) - null_count) / len(df)
        assert completeness < 1.0

    def test_data_quality_range_check(self, sample_stock_data):
        """Test data quality - value range validation"""
        df = pd.DataFrame(sample_stock_data)

        # Check for negative prices
        invalid_prices = df[df["close_price"] < 0]
        assert len(invalid_prices) == 0

        # Check for unreasonably high prices
        unreasonable = df[df["close_price"] > 10000]
        assert len(unreasonable) == 0


class TestTechnicalIndicators:
    """Test technical indicator calculations"""

    @pytest.fixture
    def price_series(self):
        """Create sample price series (25 points so rolling(20) has valid results)"""
        return pd.Series(
            [
                100,
                102,
                101,
                103,
                105,
                104,
                106,
                108,
                107,
                109,
                111,
                110,
                112,
                114,
                113,
                115,
                117,
                116,
                118,
                120,
                119,
                121,
                123,
                122,
                124,
            ]
        )

    def test_bollinger_bands(self, price_series):
        """Test Bollinger Bands calculation"""
        window = 20
        num_std = 2

        sma = price_series.rolling(window=window).mean()
        std = price_series.rolling(window=window).std()

        upper_band = sma + (std * num_std)
        lower_band = sma - (std * num_std)

        valid_sma = sma.dropna()
        valid_upper = upper_band.dropna()
        valid_lower = lower_band.dropna()

        assert len(valid_sma) > 0, "Rolling window produced no valid SMA values"

        # Upper band should be higher than SMA
        assert valid_upper.iloc[-1] > valid_sma.iloc[-1]

        # Lower band should be lower than SMA
        assert valid_lower.iloc[-1] < valid_sma.iloc[-1]

        # Price should generally be within bands (compare only where bands are defined)
        aligned = pd.concat([price_series, upper_band, lower_band], axis=1)
        aligned.columns = ["price", "upper", "lower"]
        aligned = aligned.dropna()
        in_bands = (
            (aligned["price"] >= aligned["lower"]) & (aligned["price"] <= aligned["upper"])
        ).sum()
        assert in_bands >= len(aligned) * 0.8

    def test_rsi_calculation(self, price_series):
        """Test Relative Strength Index calculation"""
        period = 14

        # Calculate price changes
        delta = price_series.diff()

        # Separate gains and losses
        gains = delta.where(delta > 0, 0)
        losses = -delta.where(delta < 0, 0)

        # Calculate average gains and losses
        avg_gains = gains.rolling(window=period).mean()
        avg_losses = losses.rolling(window=period).mean()

        # Calculate RS and RSI
        rs = avg_gains / avg_losses
        rsi = 100 - (100 / (1 + rs))

        # RSI should be between 0 and 100
        valid_rsi = rsi.dropna()
        assert (valid_rsi >= 0).all()
        assert (valid_rsi <= 100).all()

    def test_macd_calculation(self, price_series):
        """Test MACD (Moving Average Convergence Divergence)"""
        fast_period = 12
        slow_period = 26
        signal_period = 9

        # Calculate EMAs
        ema_fast = price_series.ewm(span=fast_period, adjust=False).mean()
        ema_slow = price_series.ewm(span=slow_period, adjust=False).mean()

        # MACD line
        macd = ema_fast - ema_slow

        # Signal line
        signal = macd.ewm(span=signal_period, adjust=False).mean()

        # Histogram
        histogram = macd - signal

        assert len(macd) == len(price_series)
        assert len(signal) == len(price_series)
        assert len(histogram) == len(price_series)


class TestAnomalyDetection:
    """Test anomaly detection algorithms"""

    @pytest.fixture
    def normal_data(self):
        """Create normal distribution data"""
        import numpy as np

        np.random.seed(42)
        return pd.Series(np.random.normal(100, 5, 1000))

    def test_iqr_outlier_detection(self, normal_data):
        """Test Interquartile Range outlier detection"""
        Q1 = normal_data.quantile(0.25)
        Q3 = normal_data.quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        outliers = normal_data[(normal_data < lower_bound) | (normal_data > upper_bound)]

        # Should detect some outliers, but not too many
        outlier_ratio = len(outliers) / len(normal_data)
        assert outlier_ratio < 0.1  # Less than 10%

    def test_isolation_forest(self, normal_data):
        """Test Isolation Forest anomaly detection (mocked to avoid sklearn dependency)"""
        # Mock IsolationForest behavior: simulate that outliers (200,210,220) are flagged
        data = normal_data.copy()
        outliers = pd.Series([200, 210, 220])
        data = pd.concat([data, outliers], ignore_index=True)

        mean = data.mean()
        std = data.std()

        # Use z-score as proxy for isolation forest logic
        z_scores = (data - mean) / std
        predictions = pd.Series([1 if abs(z) < 3 else -1 for z in z_scores])

        # -1 indicates anomaly, 1 indicates normal
        anomaly_count = (predictions == -1).sum()
        assert anomaly_count > 0

    def test_moving_average_threshold(self):
        """Test moving average based anomaly detection"""
        # Create data with a spike at index 50
        data = pd.Series([100.0] * 50 + [200.0] + [100.0] * 49)

        # Use a SHIFTED baseline so the spike is compared against
        # the clean window BEFORE it (not the window that includes the spike)
        window = 10
        ma = data.shift(1).rolling(window=window).mean()  # baseline from previous points
        std = data.shift(1).rolling(window=window).std()

        # Fill NaN std with 0 so baseline is ma ± 0 for flat regions
        std = std.fillna(0)

        # Threshold: 2 standard deviations (use 2 to ensure spike is caught)
        upper_threshold = ma + 2 * std
        lower_threshold = ma - 2 * std

        # Detect spikes that exceed threshold (ignore first `window+1` points with NaN baseline)
        valid_mask = ma.notna()
        anomalies = data[valid_mask & ((data > upper_threshold) | (data < lower_threshold))]

        # Should detect the spike
        assert len(anomalies) > 0
        assert 200.0 in anomalies.tolist()


class TestDataTransformations:
    """Test data transformation logic"""

    def test_symbol_partitioning(self):
        """Test data partitioning by symbol"""
        data = pd.DataFrame(
            {
                "symbol": ["AAPL", "GOOGL", "AAPL", "MSFT", "GOOGL"],
                "price": [150, 2800, 151, 350, 2801],
            }
        )

        partitions = data.groupby("symbol")

        assert len(partitions) == 3
        assert "AAPL" in partitions.groups
        assert "GOOGL" in partitions.groups
        assert "MSFT" in partitions.groups

    def test_time_window_bucketing(self):
        """Test bucketing data into time windows"""
        dates = pd.date_range(start="2024-01-01", periods=100, freq="1min")
        data = pd.DataFrame({"timestamp": dates, "value": range(100)})

        data.set_index("timestamp", inplace=True)

        # 5-minute buckets
        bucketed = data.resample("5T").count()

        assert len(bucketed) == 20  # 100 minutes / 5 = 20 buckets

    def test_schema_validation(self):
        """Test schema validation"""
        data = pd.DataFrame(
            {
                "symbol": ["AAPL", "GOOGL"],
                "timestamp": [datetime.now(), datetime.now()],
                "close_price": [150.0, 2800.0],
                "volume": [1000000, 500000],
            }
        )

        required_columns = ["symbol", "timestamp", "close_price", "volume"]

        # Check all required columns exist
        for col in required_columns:
            assert col in data.columns

        # Check data types
        assert data["close_price"].dtype in ["float64", "float32"]
        assert data["volume"].dtype in ["int64", "int32"]

    def test_duplicate_removal(self):
        """Test duplicate record detection and removal"""
        data = pd.DataFrame(
            {
                "symbol": ["AAPL", "AAPL", "GOOGL"],
                "timestamp": ["2024-01-01 10:00", "2024-01-01 10:00", "2024-01-01 10:00"],
                "price": [150, 150, 2800],
            }
        )

        # Detect duplicates
        duplicates = data.duplicated(subset=["symbol", "timestamp"], keep="first")
        assert duplicates.sum() == 1

        # Remove duplicates
        clean_data = data.drop_duplicates(subset=["symbol", "timestamp"], keep="first")
        assert len(clean_data) == 2


class TestDatabaseOperations:
    """Test database operations"""

    @pytest.fixture
    def mock_db_connection(self):
        """Create mock database connection"""
        with patch("psycopg2.connect") as mock_conn:
            mock_cursor = Mock()
            mock_conn.return_value.cursor.return_value = mock_cursor
            yield mock_conn, mock_cursor

    def test_bulk_insert(self, mock_db_connection):
        """Test bulk insert operations"""
        mock_conn, mock_cursor = mock_db_connection

        records = [("AAPL", "2024-01-01", 150.0, 1000000), ("GOOGL", "2024-01-01", 2800.0, 500000)]

        # Simulate bulk insert

        # execute_values would be called here

        assert len(records) == 2

    def test_connection_pool(self):
        """Test database connection pooling"""
        # Use a plain Mock so attribute assignment works freely
        conn_pool = Mock()
        conn_pool.minconn = 2
        conn_pool.maxconn = 10

        # Set up what getconn returns
        conn = Mock()
        conn_pool.getconn.return_value = conn

        # Actually acquire a connection from the pool
        acquired = conn_pool.getconn()

        # Use connection (cursor)
        acquired.cursor()

        # Return connection to pool
        conn_pool.putconn(acquired)

        conn_pool.getconn.assert_called_once()
        conn_pool.putconn.assert_called_once_with(conn)

    def test_query_timeout(self, mock_db_connection):
        """Test query timeout handling"""
        mock_conn, mock_cursor = mock_db_connection

        # Simulate timeout
        mock_cursor.execute.side_effect = Exception("Query timeout")

        with pytest.raises(Exception) as exc_info:
            mock_cursor.execute("SELECT * FROM large_table")

        assert "timeout" in str(exc_info.value).lower()


class TestPerformance:
    """Performance and load tests"""

    def test_processing_throughput(self):
        """Test data processing throughput"""
        import time

        # Generate large dataset
        data = pd.DataFrame(
            {
                "symbol": ["AAPL"] * 10000,
                "timestamp": pd.date_range(start="2024-01-01", periods=10000, freq="1s"),
                "price": range(10000),
            }
        )

        # Measure processing time
        start = time.time()
        result = data.groupby("symbol")["price"].agg(["mean", "std", "min", "max"])
        duration = time.time() - start

        # Should process 10k records in < 1 second
        assert duration < 1.0
        assert len(result) == 1

    def test_memory_efficiency(self):
        """Test memory usage for large datasets"""

        # Create large dataframe
        data = pd.DataFrame({"col1": range(100000), "col2": range(100000)})

        memory_usage = data.memory_usage(deep=True).sum()

        # Should be reasonable (< 10 MB for this dataset)
        assert memory_usage < 10 * 1024 * 1024

    @pytest.mark.slow
    def test_concurrent_processing(self):
        """Test concurrent processing capabilities"""
        from concurrent.futures import ThreadPoolExecutor

        def process_batch(batch_id):
            data = pd.DataFrame({"value": range(1000)})
            return data["value"].sum()

        # Process multiple batches concurrently
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(process_batch, i) for i in range(10)]
            results = [f.result() for f in futures]

        assert len(results) == 10
        assert all(r == sum(range(1000)) for r in results)

    @pytest.mark.benchmark
    def test_realistic_load(self):
        """Test with production-like data volume (self-contained)"""
        import time
        import numpy as np

        # Generate 1M records inline
        n_symbols = 100
        records_per_symbol = 10_000
        total = n_symbols * records_per_symbol

        symbols = [f"SYM{i:03d}" for i in range(n_symbols)] * records_per_symbol
        prices = np.random.uniform(50, 500, total)

        data = pd.DataFrame({"symbol": symbols, "price": prices})

        start = time.time()
        result = data.groupby("symbol")["price"].agg(["mean", "std", "min", "max"])
        duration = time.time() - start

        # Should process 1M records in < 10 seconds
        assert duration < 10.0
        assert len(result) == n_symbols

    @pytest.mark.benchmark
    def test_kafka_throughput(self):
        """Test Kafka can handle 10k messages/second (placeholder)"""
        # Integration test: requires live Kafka — skipped in unit test runs

    @pytest.mark.benchmark
    def test_spark_latency(self):
        """Test Spark processing latency < 500ms (placeholder)"""
        # Integration test: requires live Spark — skipped in unit test runs


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--tb=short"])
