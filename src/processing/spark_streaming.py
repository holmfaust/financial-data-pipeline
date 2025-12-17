"""
Spark Streaming Job - Real-time processing of financial data
Includes: windowed aggregations, technical indicators, anomaly detection
FIXED: Removed unsupported row-based window functions for streaming
"""

import os
import logging
from datetime import datetime
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, stddev,
    min as spark_min, max as spark_max, sum as spark_sum,
    lag, lead, when, abs as spark_abs, lit, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType
)
from pyspark.sql.window import Window
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


class FinancialStreamProcessor:
    """Spark Streaming processor for financial data"""
    
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.db_url = f"jdbc:postgresql://{os.getenv('DB_HOST', 'localhost')}:" \
                      f"{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'financial_data')}"
        self.db_properties = {
            "user": os.getenv('DB_USER', 'dataeng'),
            "password": os.getenv('DB_PASSWORD', 'dataeng123'),
            "driver": "org.postgresql.Driver"
        }
        
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with required configurations"""
        spark = SparkSession.builder \
            .appName("FinancialDataStreamProcessor") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "org.postgresql:postgresql:42.6.0") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        return spark
    
    def get_stock_schema(self) -> StructType:
        """Define schema for stock data"""
        return StructType([
            StructField("symbol", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("open_price", DoubleType(), True),
            StructField("high_price", DoubleType(), True),
            StructField("low_price", DoubleType(), True),
            StructField("close_price", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("source", StringType(), True)
        ])
    
    def read_kafka_stream(self, topic: str):
        """Read streaming data from Kafka"""
        logger.info(f"Reading from Kafka topic: {topic}")
        
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    
    def process_stock_stream(self):
        """Main processing pipeline for stock data"""
        
        # Read from Kafka
        raw_stream = self.read_kafka_stream("stock-prices-raw")
        
        # Parse JSON and extract fields
        stock_schema = self.get_stock_schema()
        
        parsed_stream = raw_stream.select(
            from_json(col("value").cast("string"), stock_schema).alias("data")
        ).select("data.*")
        
        # Convert timestamp
        parsed_stream = parsed_stream.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        )
        
        # Add watermark for late data handling (5 minutes)
        watermarked_stream = parsed_stream.withWatermark("timestamp", "5 minutes")
        
        # 1-minute window aggregations
        one_min_agg = self._calculate_1min_metrics(watermarked_stream)
        
        # 5-minute window aggregations (simplified for streaming)
        five_min_agg = self._calculate_5min_metrics(watermarked_stream)
        
        # Anomaly detection (using time windows instead of row windows)
        anomalies = self._detect_anomalies(watermarked_stream)
        
        # Write streams to database
        query1 = self._write_to_postgres(
            one_min_agg,
            "stock_metrics_1min",
            "1min_checkpoint"
        )
        
        query2 = self._write_to_postgres(
            five_min_agg,
            "stock_metrics_5min",
            "5min_checkpoint"
        )
        
        query3 = self._write_to_postgres(
            anomalies,
            "price_anomalies",
            "anomalies_checkpoint"
        )
        
        # Also write raw data
        query4 = self._write_raw_to_postgres(
            parsed_stream,
            "stock_prices_raw",
            "raw_checkpoint"
        )
        
        return [query1, query2, query3, query4]
    
    def _calculate_1min_metrics(self, stream):
        """Calculate 1-minute window aggregations"""
        
        agg_stream = stream.groupBy(
            window(col("timestamp"), "1 minute"),
            col("symbol")
        ).agg(
            spark_min("low_price").alias("low_price"),
            spark_max("high_price").alias("high_price"),
            avg("close_price").alias("avg_price"),
            spark_sum("volume").alias("volume"),
            count("*").alias("tick_count")
        )
        
        # Add derived columns
        result = agg_stream.select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("low_price").alias("open_price"),  # Simplified
            col("high_price"),
            col("low_price"),
            col("avg_price").alias("close_price"),
            col("avg_price"),
            col("volume"),
            col("tick_count"),
            lit(None).cast(DoubleType()).alias("price_change"),
            lit(None).cast(DoubleType()).alias("price_change_pct")
        )
        
        return result
    
    def _calculate_5min_metrics(self, stream):
        """
        Calculate 5-minute window aggregations
        Note: Moving averages (SMA) require historical data and row-based windows,
        which are not supported in streaming. These should be calculated in batch
        or using a separate stateful aggregation approach.
        """
        
        # Basic aggregations using time windows (supported in streaming)
        agg_stream = stream.groupBy(
            window(col("timestamp"), "5 minutes"),
            col("symbol")
        ).agg(
            spark_min("low_price").alias("low_price"),
            spark_max("high_price").alias("high_price"),
            avg("close_price").alias("avg_price"),
            stddev("close_price").alias("volatility"),
            spark_sum("volume").alias("volume"),
            count("*").alias("tick_count")
        )
        
        result = agg_stream.select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("low_price").alias("open_price"),
            col("high_price"),
            col("low_price"),
            col("avg_price").alias("close_price"),
            col("avg_price"),
            col("volume"),
            col("volatility"),
            col("tick_count"),
            # SMA/EMA calculations need to be done in batch or using mapGroupsWithState
            lit(None).cast(DoubleType()).alias("sma_20"),
            lit(None).cast(DoubleType()).alias("ema_20")
        )
        
        return result
    
    def _detect_anomalies(self, stream):
        """
        Detect price anomalies using time-windowed statistics
        Instead of row-based windows, we use longer time windows to calculate baseline
        """
        
        # Calculate 10-minute rolling statistics for baseline
        baseline = stream.groupBy(
            window(col("timestamp"), "10 minutes", "1 minute"),  # 10-min window, 1-min slide
            col("symbol")
        ).agg(
            avg("close_price").alias("mean_price"),
            stddev("close_price").alias("std_price")
        )
        
        # Join current prices with baseline
        # For streaming, we need to use a different approach
        # Here's a simplified version using just the current window
        current_window = stream.groupBy(
            window(col("timestamp"), "1 minute"),
            col("symbol")
        ).agg(
            avg("close_price").alias("current_price"),
            spark_min("close_price").alias("min_price"),
            spark_max("close_price").alias("max_price")
        )
        
        # Calculate price volatility within the window
        anomalies = current_window.filter(
            spark_abs(col("max_price") - col("min_price")) / col("current_price") > 0.05  # 5% swing
        )
        
        result = anomalies.select(
            col("symbol"),
            col("window.start").alias("timestamp"),
            lit("price_volatility").alias("anomaly_type"),
            col("current_price").alias("expected_price"),
            col("max_price").alias("actual_price"),
            ((col("max_price") - col("min_price")) / col("current_price") * 100)
                .alias("deviation_pct"),
            when((col("max_price") - col("min_price")) / col("current_price") > 0.1,
                 "high").otherwise("medium").alias("severity")
        )
        
        return result
    
    def _write_to_postgres(self, stream, table_name: str, checkpoint_location: str):
        """Write streaming data to PostgreSQL"""
        
        def write_batch(batch_df, batch_id):
            """Function to write each micro-batch"""
            try:
                if batch_df.count() > 0:
                    batch_df.write \
                        .mode("append") \
                        .jdbc(self.db_url, table_name, properties=self.db_properties)
                    logger.info(f"Batch {batch_id} written to {table_name}: {batch_df.count()} records")
            except Exception as e:
                logger.error(f"Error writing batch {batch_id} to {table_name}: {e}")
        
        query = stream.writeStream \
            .foreachBatch(write_batch) \
            .outputMode("update") \
            .option("checkpointLocation", f"/tmp/spark-checkpoint/{checkpoint_location}") \
            .start()
        
        return query
    
    def _write_raw_to_postgres(self, stream, table_name: str, checkpoint_location: str):
        """Write raw streaming data to PostgreSQL"""
        
        def write_batch(batch_df, batch_id):
            try:
                if batch_df.count() > 0:
                    batch_df.write \
                        .mode("append") \
                        .jdbc(self.db_url, table_name, properties=self.db_properties)
                    logger.info(f"Raw batch {batch_id} written: {batch_df.count()} records")
            except Exception as e:
                logger.error(f"Error writing raw batch {batch_id}: {e}")
        
        query = stream.writeStream \
            .foreachBatch(write_batch) \
            .outputMode("append") \
            .option("checkpointLocation", f"/tmp/spark-checkpoint/{checkpoint_location}") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        return query
    
    def run(self):
        """Start all streaming queries"""
        logger.info("Starting Spark streaming jobs...")
        
        try:
            queries = self.process_stock_stream()
            
            # Wait for all queries
            for query in queries:
                logger.info(f"Query started with ID: {query.id}")
            
            # Await termination
            self.spark.streams.awaitAnyTermination()
            
        except KeyboardInterrupt:
            logger.info("Stopping streaming jobs...")
        except Exception as e:
            logger.error(f"Error in streaming job: {e}")
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    processor = FinancialStreamProcessor()
    processor.run()