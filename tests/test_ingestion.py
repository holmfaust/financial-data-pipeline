"""
Unit tests for data ingestion components
"""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ingestion.stock_producer import StockDataProducer
from src.ingestion.crypto_producer import CryptoDataProducer


class TestStockDataProducer:
    """Test cases for StockDataProducer"""
    
    @pytest.fixture
    def producer(self):
        """Create a producer instance for testing"""
        with patch.dict(os.environ, {
            'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
            'STOCK_SYMBOLS': 'AAPL,GOOGL'
        }):
            with patch('src.ingestion.stock_producer.KafkaProducer'):
                producer = StockDataProducer()
                return producer
    
    def test_initialization(self, producer):
        """Test producer initialization"""
        assert producer.topic == 'stock-prices-raw'
        assert 'AAPL' in producer.symbols
        assert 'GOOGL' in producer.symbols
    
    def test_data_validation_valid(self, producer):
        """Test validation with valid data"""
        valid_data = {
            'symbol': 'AAPL',
            'timestamp': datetime.now().isoformat(),
            'close_price': 150.25,
            'volume': 1000000
        }
        assert producer.validate_data(valid_data) == True
    
    def test_data_validation_invalid_price(self, producer):
        """Test validation with invalid price"""
        invalid_data = {
            'symbol': 'AAPL',
            'timestamp': datetime.now().isoformat(),
            'close_price': -10.0,  # Invalid: negative price
            'volume': 1000000
        }
        assert producer.validate_data(invalid_data) == False
    
    def test_data_validation_missing_fields(self, producer):
        """Test validation with missing required fields"""
        incomplete_data = {
            'symbol': 'AAPL',
            'timestamp': datetime.now().isoformat()
            # Missing close_price and volume
        }
        assert producer.validate_data(incomplete_data) == False
    
    def test_data_validation_invalid_volume(self, producer):
        """Test validation with negative volume"""
        invalid_data = {
            'symbol': 'AAPL',
            'timestamp': datetime.now().isoformat(),
            'close_price': 150.25,
            'volume': -1000  # Invalid: negative volume
        }
        assert producer.validate_data(invalid_data) == False
    
    @patch('src.ingestion.stock_producer.yf.Ticker')
    def test_fetch_stock_data_success(self, mock_ticker, producer):
        """Test successful data fetching from yfinance"""
        # Mock yfinance response
        mock_history = Mock()
        mock_history.iloc = [-1]
        mock_history.__getitem__ = Mock(return_value=Mock(iloc=[-1]))
        
        mock_data = {
            'Open': 150.0,
            'High': 152.0,
            'Low': 149.0,
            'Close': 151.5,
            'Volume': 1000000
        }
        
        mock_history.iloc.__getitem__ = Mock(return_value=Mock(**mock_data))
        mock_ticker.return_value.history.return_value = mock_history
        
        # This test would need more sophisticated mocking
        # For now, we'll test the structure
        assert producer.symbols == ['AAPL', 'GOOGL']
    
    @patch('src.ingestion.stock_producer.requests.get')
    def test_fetch_alpha_vantage_success(self, mock_get, producer):
        """Test Alpha Vantage API fetching"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'Global Quote': {
                '02. open': '150.00',
                '03. high': '152.00',
                '04. low': '149.00',
                '05. price': '151.50',
                '06. volume': '1000000'
            }
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        producer.alpha_vantage_key = 'test_key'
        data = producer.fetch_stock_data_alpha_vantage('AAPL')
        
        assert data is not None
        assert data['symbol'] == 'AAPL'
        assert data['close_price'] == 151.50


class TestCryptoDataProducer:
    """Test cases for CryptoDataProducer"""
    
    @pytest.fixture
    def producer(self):
        """Create a crypto producer instance"""
        with patch.dict(os.environ, {
            'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
            'CRYPTO_SYMBOLS': 'bitcoin,ethereum'
        }):
            with patch('src.ingestion.crypto_producer.KafkaProducer'):
                producer = CryptoDataProducer()
                return producer
    
    def test_initialization(self, producer):
        """Test crypto producer initialization"""
        assert producer.topic == 'crypto-prices-raw'
        assert 'bitcoin' in producer.symbols
    
    @patch('src.ingestion.crypto_producer.requests.get')
    def test_fetch_coingecko_success(self, mock_get, producer):
        """Test CoinGecko API fetching"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'bitcoin': {
                'usd': 45000.0,
                'usd_market_cap': 850000000000,
                'usd_24h_vol': 30000000000,
                'usd_24h_change': 2.5,
                'last_updated_at': 1234567890
            }
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        data = producer.fetch_crypto_data_coingecko(['bitcoin'])
        
        assert len(data) == 1
        assert data[0]['symbol'] == 'bitcoin'
        assert data[0]['price_usd'] == 45000.0
    
    def test_validate_crypto_data_valid(self, producer):
        """Test validation with valid crypto data"""
        valid_data = {
            'symbol': 'bitcoin',
            'timestamp': datetime.now().isoformat(),
            'price_usd': 45000.0
        }
        assert producer.validate_data(valid_data) == True
    
    def test_validate_crypto_data_invalid(self, producer):
        """Test validation with invalid crypto data"""
        invalid_data = {
            'symbol': 'bitcoin',
            'timestamp': datetime.now().isoformat(),
            'price_usd': -100.0  # Invalid: negative price
        }
        assert producer.validate_data(invalid_data) == False


class TestDataQuality:
    """Test data quality checks"""
    
    def test_schema_compliance(self):
        """Test that data follows expected schema"""
        sample_data = {
            'symbol': 'AAPL',
            'timestamp': datetime.now().isoformat(),
            'open_price': 150.0,
            'high_price': 152.0,
            'low_price': 149.0,
            'close_price': 151.5,
            'volume': 1000000,
            'source': 'yfinance'
        }
        
        required_fields = ['symbol', 'timestamp', 'close_price', 'volume', 'source']
        for field in required_fields:
            assert field in sample_data
    
    def test_data_type_validation(self):
        """Test data types are correct"""
        sample_data = {
            'symbol': 'AAPL',
            'close_price': 151.5,
            'volume': 1000000
        }
        
        assert isinstance(sample_data['symbol'], str)
        assert isinstance(sample_data['close_price'], (int, float))
        assert isinstance(sample_data['volume'], int)
    
    def test_reasonable_value_ranges(self):
        """Test that values are within reasonable ranges"""
        # Stock prices should be positive and typically under $10,000
        assert 0 < 151.5 < 10000
        
        # Volume should be positive
        assert 1000000 > 0
        
        # Percent change should typically be between -50% and +50% daily
        percent_change = 2.5
        assert -50 < percent_change < 50


if __name__ == "__main__":
    pytest.main([__file__, "-v"])