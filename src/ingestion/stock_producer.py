"""
Stock Data Producer - Ingests real-time stock prices and publishes to Kafka
"""

import json
import time
import logging
from datetime import datetime
from typing import Dict, List
import os
from dotenv import load_dotenv

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


class StockDataProducer:
    """Producer for streaming stock market data to Kafka"""
    
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_STOCK_TOPIC', 'stock-prices-raw')
        self.symbols = os.getenv('STOCK_SYMBOLS', 'AAPL,GOOGL,MSFT').split(',')
        self.alpha_vantage_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        
        # Initialize Kafka Producer
        self.producer = self._create_producer()
        
    def _create_producer(self) -> KafkaProducer:
        """Create and configure Kafka producer"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Kafka producer connected to {self.bootstrap_servers}")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise
    
    def fetch_stock_data_yfinance(self, symbol: str) -> Dict:
        """Fetch real-time stock data using yfinance"""
        try:
            ticker = yf.Ticker(symbol)
            # Get latest intraday data
            data = ticker.history(period='1d', interval='1m')
            
            if data.empty:
                logger.warning(f"No data available for {symbol}")
                return None
            
            latest = data.iloc[-1]
            
            stock_data = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'open_price': float(latest['Open']),
                'high_price': float(latest['High']),
                'low_price': float(latest['Low']),
                'close_price': float(latest['Close']),
                'volume': int(latest['Volume']),
                'source': 'yfinance',
                'data_quality': {
                    'is_valid': True,
                    'has_nulls': False
                }
            }
            
            return stock_data
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None
    
    def fetch_stock_data_alpha_vantage(self, symbol: str) -> Dict:
        """Fetch stock data from Alpha Vantage API (alternative source)"""
        if not self.alpha_vantage_key:
            logger.warning("Alpha Vantage API key not configured")
            return None
            
        try:
            url = f'https://www.alphavantage.co/query'
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol,
                'apikey': self.alpha_vantage_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'Global Quote' not in data:
                logger.warning(f"Invalid response for {symbol}")
                return None
            
            quote = data['Global Quote']
            
            stock_data = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'open_price': float(quote.get('02. open', 0)),
                'high_price': float(quote.get('03. high', 0)),
                'low_price': float(quote.get('04. low', 0)),
                'close_price': float(quote.get('05. price', 0)),
                'volume': int(quote.get('06. volume', 0)),
                'source': 'alpha_vantage',
                'data_quality': {
                    'is_valid': True,
                    'has_nulls': False
                }
            }
            
            return stock_data
            
        except Exception as e:
            logger.error(f"Error fetching Alpha Vantage data for {symbol}: {e}")
            return None
    
    def validate_data(self, data: Dict) -> bool:
        """Validate stock data before publishing"""
        required_fields = ['symbol', 'timestamp', 'close_price', 'volume']
        
        # Check required fields
        if not all(field in data for field in required_fields):
            logger.error(f"Missing required fields in data: {data}")
            return False
        
        # Check for reasonable values
        if data['close_price'] <= 0:
            logger.error(f"Invalid price for {data['symbol']}: {data['close_price']}")
            return False
        
        if data['volume'] < 0:
            logger.error(f"Invalid volume for {data['symbol']}: {data['volume']}")
            return False
        
        return True
    
    def publish_to_kafka(self, data: Dict) -> bool:
        """Publish stock data to Kafka topic"""
        try:
            if not self.validate_data(data):
                return False
            
            future = self.producer.send(
                self.topic,
                key=data['symbol'],
                value=data
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Published {data['symbol']} to {record_metadata.topic} "
                f"partition {record_metadata.partition} offset {record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"Kafka error publishing {data['symbol']}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error publishing {data['symbol']}: {e}")
            return False
    
    def run_continuous(self, interval_seconds: int = 60):
        """Run producer continuously"""
        logger.info(f"Starting stock data producer for symbols: {self.symbols}")
        logger.info(f"Publishing interval: {interval_seconds} seconds")
        
        try:
            while True:
                start_time = time.time()
                
                for symbol in self.symbols:
                    # Try yfinance first
                    data = self.fetch_stock_data_yfinance(symbol.strip())
                    
                    # Fallback to Alpha Vantage if yfinance fails
                    if not data and self.alpha_vantage_key:
                        data = self.fetch_stock_data_alpha_vantage(symbol.strip())
                    
                    if data:
                        self.publish_to_kafka(data)
                    
                    # Small delay between symbols to avoid rate limits
                    time.sleep(0.5)
                
                # Ensure we maintain the desired interval
                elapsed = time.time() - start_time
                sleep_time = max(0, interval_seconds - elapsed)
                
                if sleep_time > 0:
                    logger.info(f"Waiting {sleep_time:.2f} seconds until next batch...")
                    time.sleep(sleep_time)
                    
        except KeyboardInterrupt:
            logger.info("Shutting down producer...")
        finally:
            self.producer.close()
            logger.info("Producer closed")
    
    def run_once(self):
        """Run producer once (useful for testing)"""
        logger.info("Running single batch of stock data collection...")
        
        for symbol in self.symbols:
            data = self.fetch_stock_data_yfinance(symbol.strip())
            if data:
                self.publish_to_kafka(data)
        
        self.producer.flush()
        logger.info("Single batch completed")


if __name__ == "__main__":
    producer = StockDataProducer()
    
    # Run continuously (comment out for testing)
    interval = int(os.getenv('INGESTION_INTERVAL_SECONDS', 60))
    producer.run_continuous(interval_seconds=interval)
    
    # For testing, use:
    # producer.run_once()