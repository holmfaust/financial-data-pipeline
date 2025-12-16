"""
Cryptocurrency Data Producer - Ingests real-time crypto prices and publishes to Kafka
"""

import json
import time
import logging
from datetime import datetime
from typing import Dict, List
import os
from dotenv import load_dotenv

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


class CryptoDataProducer:
    """Producer for streaming cryptocurrency data to Kafka"""
    
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_CRYPTO_TOPIC', 'crypto-prices-raw')
        self.symbols = os.getenv('CRYPTO_SYMBOLS', 'bitcoin,ethereum').split(',')
        
        # CoinGecko API endpoints
        self.coingecko_base_url = 'https://api.coingecko.com/api/v3'
        
        # Initialize Kafka Producer
        self.producer = self._create_producer()
        
    def _create_producer(self) -> KafkaProducer:
        """Create and configure Kafka producer"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Kafka producer connected to {self.bootstrap_servers}")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise
    
    def fetch_crypto_data_coingecko(self, symbols: List[str]) -> List[Dict]:
        """Fetch cryptocurrency data from CoinGecko API"""
        try:
            # CoinGecko allows batch requests
            symbols_str = ','.join(symbols)
            url = f'{self.coingecko_base_url}/simple/price'
            
            params = {
                'ids': symbols_str,
                'vs_currencies': 'usd',
                'include_market_cap': 'true',
                'include_24hr_vol': 'true',
                'include_24hr_change': 'true',
                'include_last_updated_at': 'true'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            crypto_records = []
            current_time = datetime.now().isoformat()
            
            for symbol in symbols:
                if symbol not in data:
                    logger.warning(f"No data available for {symbol}")
                    continue
                
                coin_data = data[symbol]
                
                crypto_record = {
                    'symbol': symbol,
                    'timestamp': current_time,
                    'price_usd': coin_data.get('usd', 0),
                    'market_cap': coin_data.get('usd_market_cap', 0),
                    'volume_24h': coin_data.get('usd_24h_vol', 0),
                    'percent_change_24h': coin_data.get('usd_24h_change', 0),
                    'last_updated': coin_data.get('last_updated_at', 0),
                    'source': 'coingecko',
                    'data_quality': {
                        'is_valid': True,
                        'has_nulls': False
                    }
                }
                
                crypto_records.append(crypto_record)
            
            return crypto_records
            
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error fetching crypto data: {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching crypto data: {e}")
            return []
    
    def fetch_crypto_data_binance(self, symbol: str) -> Dict:
        """Fetch crypto data from Binance API (alternative source)"""
        try:
            # Binance uses different symbol format (e.g., BTCUSDT)
            binance_symbol_map = {
                'bitcoin': 'BTCUSDT',
                'ethereum': 'ETHUSDT',
                'cardano': 'ADAUSDT',
                'solana': 'SOLUSDT',
                'polkadot': 'DOTUSDT'
            }
            
            binance_symbol = binance_symbol_map.get(symbol.lower())
            if not binance_symbol:
                logger.warning(f"No Binance mapping for {symbol}")
                return None
            
            url = 'https://api.binance.com/api/v3/ticker/24hr'
            params = {'symbol': binance_symbol}
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            crypto_record = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'price_usd': float(data.get('lastPrice', 0)),
                'volume_24h': float(data.get('volume', 0)) * float(data.get('lastPrice', 0)),
                'percent_change_24h': float(data.get('priceChangePercent', 0)),
                'high_24h': float(data.get('highPrice', 0)),
                'low_24h': float(data.get('lowPrice', 0)),
                'source': 'binance',
                'data_quality': {
                    'is_valid': True,
                    'has_nulls': False
                }
            }
            
            return crypto_record
            
        except Exception as e:
            logger.error(f"Error fetching Binance data for {symbol}: {e}")
            return None
    
    def validate_data(self, data: Dict) -> bool:
        """Validate crypto data before publishing"""
        required_fields = ['symbol', 'timestamp', 'price_usd']
        
        # Check required fields
        if not all(field in data for field in required_fields):
            logger.error(f"Missing required fields in data: {data}")
            return False
        
        # Check for reasonable values
        if data['price_usd'] <= 0:
            logger.error(f"Invalid price for {data['symbol']}: {data['price_usd']}")
            return False
        
        return True
    
    def publish_to_kafka(self, data: Dict) -> bool:
        """Publish crypto data to Kafka topic"""
        try:
            if not self.validate_data(data):
                return False
            
            future = self.producer.send(
                self.topic,
                key=data['symbol'],
                value=data
            )
            
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Published {data['symbol']} (${data['price_usd']:.2f}) to "
                f"{record_metadata.topic} partition {record_metadata.partition}"
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
        logger.info(f"Starting crypto data producer for symbols: {self.symbols}")
        logger.info(f"Publishing interval: {interval_seconds} seconds")
        
        try:
            while True:
                start_time = time.time()
                
                # Fetch all crypto data in one batch (CoinGecko supports this)
                crypto_data_list = self.fetch_crypto_data_coingecko(
                    [s.strip() for s in self.symbols]
                )
                
                # Publish each record
                for data in crypto_data_list:
                    self.publish_to_kafka(data)
                
                # If CoinGecko fails, fallback to Binance for individual coins
                if not crypto_data_list:
                    logger.warning("CoinGecko batch failed, trying Binance...")
                    for symbol in self.symbols:
                        data = self.fetch_crypto_data_binance(symbol.strip())
                        if data:
                            self.publish_to_kafka(data)
                        time.sleep(0.5)  # Rate limit protection
                
                # Maintain interval
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
        logger.info("Running single batch of crypto data collection...")
        
        crypto_data_list = self.fetch_crypto_data_coingecko(
            [s.strip() for s in self.symbols]
        )
        
        for data in crypto_data_list:
            self.publish_to_kafka(data)
        
        self.producer.flush()
        logger.info("Single batch completed")


if __name__ == "__main__":
    producer = CryptoDataProducer()
    
    # Run continuously
    interval = int(os.getenv('INGESTION_INTERVAL_SECONDS', 60))
    producer.run_continuous(interval_seconds=interval)
    
    # For testing, use:
    # producer.run_once()