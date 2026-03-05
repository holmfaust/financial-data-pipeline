"""
Unit tests for data storage components (DatabaseManager and CacheManager)
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.storage.db_manager import DatabaseManager, CacheManager


class TestDatabaseManager:
    """Test cases for DatabaseManager"""

    @pytest.fixture
    def db_manager(self):
        """Create a DatabaseManager instance with mocked connection"""
        with patch.dict(
            os.environ,
            {
                "DB_HOST": "localhost",
                "DB_PORT": "5432",
                "DB_NAME": "financial_data",
                "DB_USER": "dataeng",
                "DB_PASSWORD": "dataeng123",
            },
        ):
            return DatabaseManager()

    def test_initialization(self, db_manager):
        """Test that DatabaseManager initializes with correct config"""
        assert db_manager.conn_params["host"] == "localhost"
        assert db_manager.conn_params["port"] == "5432"
        assert db_manager.conn_params["database"] == "financial_data"
        assert db_manager.conn_params["user"] == "dataeng"

    @patch("src.storage.db_manager.psycopg2.connect")
    def test_get_latest_prices_no_filter(self, mock_connect, db_manager):
        """Test fetching latest prices without symbol filter"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {
                "symbol": "AAPL",
                "close_price": 150.25,
                "volume": 1000000,
                "timestamp": datetime.now(),
            },
            {
                "symbol": "GOOGL",
                "close_price": 2800.50,
                "volume": 500000,
                "timestamp": datetime.now(),
            },
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_connect.return_value.__exit__ = Mock(return_value=False)

        with patch.object(db_manager, "get_connection") as mock_get_conn:
            mock_get_conn.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = Mock(return_value=False)

            result = db_manager.get_latest_prices()
            assert result is not None

    @patch("src.storage.db_manager.psycopg2.connect")
    def test_get_latest_prices_with_symbols(self, mock_connect, db_manager):
        """Test fetching latest prices with specific symbols"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {
                "symbol": "AAPL",
                "close_price": 150.25,
                "volume": 1000000,
                "timestamp": datetime.now(),
            }
        ]
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(db_manager, "get_connection") as mock_get_conn:
            mock_get_conn.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = Mock(return_value=False)

            result = db_manager.get_latest_prices(symbols=["AAPL"])
            assert result is not None

    @patch("src.storage.db_manager.psycopg2.connect")
    def test_get_data_quality_metrics(self, mock_connect, db_manager):
        """Test data quality metrics calculation"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Mock multiple fetchone calls in sequence
        mock_cursor.fetchone.side_effect = [
            {"count": 1000},  # total_stock_records
            {"count": 500},  # total_crypto_records
            {"latest_timestamp": datetime.now() - timedelta(minutes=2)},  # data_freshness
            {"count": 0},  # records_with_nulls
        ]
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(db_manager, "get_connection") as mock_get_conn:
            mock_get_conn.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = Mock(return_value=False)

            metrics = db_manager.get_data_quality_metrics()
            assert "total_stock_records" in metrics
            assert "total_crypto_records" in metrics
            assert "records_with_nulls" in metrics

    @patch("src.storage.db_manager.psycopg2.connect")
    def test_insert_pipeline_run(self, mock_connect, db_manager):
        """Test pipeline run logging"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(db_manager, "get_connection") as mock_get_conn:
            mock_get_conn.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = Mock(return_value=False)

            # Should not raise
            db_manager.insert_pipeline_run(
                pipeline_name="test_pipeline",
                run_id="run_001",
                status="success",
                records_processed=100,
                records_failed=0,
            )

    @patch("src.storage.db_manager.psycopg2.connect")
    def test_get_historical_data(self, mock_connect, db_manager):
        """Test historical data retrieval"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {"symbol": "AAPL", "close_price": 150.0, "timestamp": datetime.now()}
        ]
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(db_manager, "get_connection") as mock_get_conn:
            mock_get_conn.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = Mock(return_value=False)

            result = db_manager.get_historical_data("AAPL", days=7)
            assert result is not None

    @patch("src.storage.db_manager.psycopg2.connect")
    def test_get_anomalies_with_symbol(self, mock_connect, db_manager):
        """Test anomaly retrieval for a specific symbol"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(db_manager, "get_connection") as mock_get_conn:
            mock_get_conn.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = Mock(return_value=False)

            result = db_manager.get_anomalies(symbol="AAPL", hours=24)
            assert isinstance(result, list)

    @patch("src.storage.db_manager.psycopg2.connect")
    def test_database_connection_error(self, mock_connect, db_manager):
        """Test that database errors are properly raised"""
        mock_connect.side_effect = Exception("Connection refused")

        with patch.object(db_manager, "get_connection") as mock_get_conn:
            mock_get_conn.return_value.__enter__ = Mock(side_effect=Exception("Connection refused"))
            mock_get_conn.return_value.__exit__ = Mock(return_value=False)

            with pytest.raises(Exception):
                db_manager.get_latest_prices()


class TestCacheManager:
    """Test cases for CacheManager"""

    @pytest.fixture
    def cache_manager(self):
        """Create a CacheManager instance with mocked Redis"""
        with patch("src.storage.db_manager.redis.Redis") as mock_redis_class:
            mock_redis_instance = MagicMock()
            mock_redis_class.return_value = mock_redis_instance
            manager = CacheManager()
            manager.redis_client = mock_redis_instance
            return manager

    def test_set_latest_price(self, cache_manager):
        """Test caching a stock price"""
        cache_manager.set_latest_price("AAPL", 150.25, ttl=300)
        cache_manager.redis_client.setex.assert_called_once_with("price:AAPL", 300, "150.25")

    def test_get_latest_price_exists(self, cache_manager):
        """Test retrieving a cached price"""
        cache_manager.redis_client.get.return_value = "150.25"
        price = cache_manager.get_latest_price("AAPL")
        assert price == 150.25
        cache_manager.redis_client.get.assert_called_once_with("price:AAPL")

    def test_get_latest_price_not_exists(self, cache_manager):
        """Test retrieving a non-existent cached price returns None"""
        cache_manager.redis_client.get.return_value = None
        price = cache_manager.get_latest_price("UNKNOWN")
        assert price is None

    def test_set_market_summary(self, cache_manager):
        """Test caching market summary"""
        summary = {"total_stocks": 7, "avg_change": 1.5, "top_gainer": "NVDA"}
        cache_manager.set_market_summary(summary, ttl=60)
        cache_manager.redis_client.setex.assert_called_once()
        call_args = cache_manager.redis_client.setex.call_args
        assert call_args[0][0] == "market:summary"
        assert call_args[0][1] == 60

    def test_get_market_summary_exists(self, cache_manager):
        """Test retrieving cached market summary"""
        import json

        summary = {"total_stocks": 7, "avg_change": 1.5}
        cache_manager.redis_client.get.return_value = json.dumps(summary)

        result = cache_manager.get_market_summary()
        assert result == summary

    def test_get_market_summary_not_exists(self, cache_manager):
        """Test retrieving non-existent market summary returns None"""
        cache_manager.redis_client.get.return_value = None
        result = cache_manager.get_market_summary()
        assert result is None

    def test_price_key_format(self, cache_manager):
        """Test that Redis keys follow expected format"""
        cache_manager.set_latest_price("NVDA", 500.0)
        call_args = cache_manager.redis_client.setex.call_args
        assert call_args[0][0] == "price:NVDA"

    def test_price_stored_as_string(self, cache_manager):
        """Test that price is stored as string in Redis"""
        cache_manager.set_latest_price("TSLA", 200.99)
        call_args = cache_manager.redis_client.setex.call_args
        stored_value = call_args[0][2]
        assert isinstance(stored_value, str)
        assert stored_value == "200.99"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
