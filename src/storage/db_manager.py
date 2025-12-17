"""
Database Manager - Handles database connections and operations
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import redis
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


class DatabaseManager:
    """Manager for PostgreSQL operations"""
    
    def __init__(self):
        self.conn_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'financial_data'),
            'user': os.getenv('DB_USER', 'dataeng'),
            'password': os.getenv('DB_PASSWORD', 'dataeng123')
        }
        
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(**self.conn_params)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def get_latest_prices(self, symbols: Optional[List[str]] = None) -> List[Dict]:
        """Get latest prices for stocks"""
        with self.get_connection() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            if symbols:
                placeholders = ','.join(['%s'] * len(symbols))
                query = f"""
                    SELECT DISTINCT ON (symbol)
                        symbol, close_price, volume, timestamp
                    FROM stock_prices_raw
                    WHERE symbol IN ({placeholders})
                    ORDER BY symbol, timestamp DESC
                """
                cur.execute(query, symbols)
            else:
                query = """
                    SELECT DISTINCT ON (symbol)
                        symbol, close_price, volume, timestamp
                    FROM stock_prices_raw
                    ORDER BY symbol, timestamp DESC
                """
                cur.execute(query)
            
            return cur.fetchall()
    
    def get_historical_data(self, symbol: str, days: int = 7) -> List[Dict]:
        """Get historical data for a symbol"""
        with self.get_connection() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            query = """
                SELECT symbol, timestamp, open_price, high_price, 
                       low_price, close_price, volume
                FROM stock_prices_raw
                WHERE symbol = %s 
                  AND timestamp >= NOW() - INTERVAL '%s days'
                ORDER BY timestamp ASC
            """
            cur.execute(query, (symbol, days))
            return cur.fetchall()
    
    def get_aggregated_metrics(self, symbol: str, window: str = '5min', 
                               limit: int = 100) -> List[Dict]:
        """Get aggregated metrics"""
        table_map = {
            '1min': 'stock_metrics_1min',
            '5min': 'stock_metrics_5min'
        }
        
        table_name = table_map.get(window, 'stock_metrics_5min')
        
        with self.get_connection() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE symbol = %s
                ORDER BY window_start DESC
                LIMIT %s
            """
            cur.execute(query, (symbol, limit))
            return cur.fetchall()
    
    def get_anomalies(self, symbol: Optional[str] = None, 
                      hours: int = 24) -> List[Dict]:
        """Get detected anomalies"""
        with self.get_connection() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            if symbol:
                query = """
                    SELECT *
                    FROM price_anomalies
                    WHERE symbol = %s
                      AND timestamp >= NOW() - INTERVAL '%s hours'
                    ORDER BY timestamp DESC
                """
                cur.execute(query, (symbol, hours))
            else:
                query = """
                    SELECT *
                    FROM price_anomalies
                    WHERE timestamp >= NOW() - INTERVAL '%s hours'
                    ORDER BY timestamp DESC
                """
                cur.execute(query, (hours,))
            
            return cur.fetchall()
    
    def insert_pipeline_run(self, pipeline_name: str, run_id: str, 
                           status: str, records_processed: int = 0,
                           records_failed: int = 0, error_message: str = None):
        """Log pipeline execution"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            query = """
                INSERT INTO pipeline_runs 
                (pipeline_name, run_id, status, records_processed, 
                 records_failed, start_time, error_message)
                VALUES (%s, %s, %s, %s, %s, NOW(), %s)
            """
            cur.execute(query, (pipeline_name, run_id, status, 
                              records_processed, records_failed, error_message))
    
    def get_data_quality_metrics(self) -> Dict:
        """Calculate data quality metrics"""
        with self.get_connection() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            metrics = {}
            
            # Record counts
            cur.execute("SELECT COUNT(*) as count FROM stock_prices_raw")
            metrics['total_stock_records'] = cur.fetchone()['count']
            
            cur.execute("SELECT COUNT(*) as count FROM crypto_prices_raw")
            metrics['total_crypto_records'] = cur.fetchone()['count']
            
            # Data freshness
            cur.execute("""
                SELECT MAX(timestamp) as latest_timestamp
                FROM stock_prices_raw
            """)
            result = cur.fetchone()
            if result['latest_timestamp']:
                metrics['data_freshness_minutes'] = \
                    (datetime.now() - result['latest_timestamp']).total_seconds() / 60
            
            # Missing data detection
            cur.execute("""
                SELECT COUNT(*) as count
                FROM stock_prices_raw
                WHERE close_price IS NULL OR volume IS NULL
            """)
            metrics['records_with_nulls'] = cur.fetchone()['count']
            
            return metrics


class CacheManager:
    """Manager for Redis caching operations"""
    
    def __init__(self):
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=0,
            decode_responses=True
        )
    
    def set_latest_price(self, symbol: str, price: float, ttl: int = 300):
        """Cache latest price with TTL"""
        key = f"price:{symbol}"
        self.redis_client.setex(key, ttl, str(price))
    
    def get_latest_price(self, symbol: str) -> Optional[float]:
        """Get cached price"""
        key = f"price:{symbol}"
        price = self.redis_client.get(key)
        return float(price) if price else None
    
    def set_market_summary(self, data: Dict, ttl: int = 60):
        """Cache market summary"""
        import json
        self.redis_client.setex('market:summary', ttl, json.dumps(data))
    
    def get_market_summary(self) -> Optional[Dict]:
        """Get cached market summary"""
        import json
        data = self.redis_client.get('market:summary')
        return json.loads(data) if data else None


if __name__ == "__main__":
    # Test database connection
    db = DatabaseManager()
    
    try:
        latest_prices = db.get_latest_prices()
        logger.info(f"Retrieved {len(latest_prices)} latest prices")
        
        metrics = db.get_data_quality_metrics()
        logger.info(f"Data quality metrics: {metrics}")
        
        # Test Redis
        cache = CacheManager()
        cache.set_latest_price('AAPL', 150.25)
        price = cache.get_latest_price('AAPL')
        logger.info(f"Cached price for AAPL: {price}")
        
    except Exception as e:
        logger.error(f"Error testing database: {e}")