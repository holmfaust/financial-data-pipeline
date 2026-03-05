"""
Data Lineage Tracking System
Tracks data flow from source to destination with full audit trail
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import hashlib
import psycopg2
from psycopg2.extras import RealDictCursor, Json
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatasetType(Enum):
    """Types of datasets in the pipeline"""

    RAW = "raw"
    PROCESSED = "processed"
    AGGREGATED = "aggregated"
    ENRICHED = "enriched"


class TransformationType(Enum):
    """Types of transformations"""

    INGESTION = "ingestion"
    FILTERING = "filtering"
    AGGREGATION = "aggregation"
    CALCULATION = "calculation"
    ENRICHMENT = "enrichment"
    VALIDATION = "validation"


@dataclass
class Dataset:
    """Represents a dataset in the pipeline"""

    id: str
    name: str
    type: DatasetType
    schema: Dict[str, str]
    location: str
    row_count: int
    created_at: datetime
    metadata: Dict[str, Any]

    def to_dict(self):
        return {**asdict(self), "type": self.type.value, "created_at": self.created_at.isoformat()}


@dataclass
class Transformation:
    """Represents a transformation between datasets"""

    id: str
    name: str
    type: TransformationType
    description: str
    input_datasets: List[str]
    output_dataset: str
    code_reference: Optional[str]
    parameters: Dict[str, Any]
    executed_at: datetime
    duration_seconds: float
    row_count_input: int
    row_count_output: int
    status: str  # success, failed, running

    def to_dict(self):
        return {
            **asdict(self),
            "type": self.type.value,
            "executed_at": self.executed_at.isoformat(),
        }


class DataLineageTracker:
    """
    Tracks data lineage throughout the pipeline
    """

    def __init__(self):
        self.conn_params = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "database": os.getenv("DB_NAME", "financial_data"),
            "user": os.getenv("DB_USER", "dataeng"),
            "password": os.getenv("DB_PASSWORD", "dataeng123"),
        }
        self._initialize_schema()

    def _get_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.conn_params)

    def _initialize_schema(self):
        """Create lineage tracking tables if they don't exist"""
        create_tables_sql = """
        -- Datasets table
        CREATE TABLE IF NOT EXISTS lineage_datasets (
            id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            type VARCHAR(50) NOT NULL,
            schema JSONB,
            location TEXT,
            row_count BIGINT,
            created_at TIMESTAMP NOT NULL,
            metadata JSONB,
            CONSTRAINT unique_dataset_name_version UNIQUE (name, created_at)
        );

        -- Transformations table
        CREATE TABLE IF NOT EXISTS lineage_transformations (
            id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            type VARCHAR(50) NOT NULL,
            description TEXT,
            input_datasets JSONB NOT NULL,
            output_dataset VARCHAR(255) NOT NULL,
            code_reference TEXT,
            parameters JSONB,
            executed_at TIMESTAMP NOT NULL,
            duration_seconds FLOAT,
            row_count_input BIGINT,
            row_count_output BIGINT,
            status VARCHAR(50) NOT NULL,
            error_message TEXT,
            FOREIGN KEY (output_dataset) REFERENCES lineage_datasets(id)
        );

        -- Lineage edges table (for graph representation)
        CREATE TABLE IF NOT EXISTS lineage_edges (
            id SERIAL PRIMARY KEY,
            source_dataset_id VARCHAR(255) NOT NULL,
            target_dataset_id VARCHAR(255) NOT NULL,
            transformation_id VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            FOREIGN KEY (source_dataset_id) REFERENCES lineage_datasets(id),
            FOREIGN KEY (target_dataset_id) REFERENCES lineage_datasets(id),
            FOREIGN KEY (transformation_id) REFERENCES lineage_transformations(id),
            CONSTRAINT unique_lineage_edge UNIQUE (source_dataset_id, target_dataset_id, transformation_id)
        );

        -- Column-level lineage
        CREATE TABLE IF NOT EXISTS lineage_columns (
            id SERIAL PRIMARY KEY,
            transformation_id VARCHAR(255) NOT NULL,
            source_dataset_id VARCHAR(255) NOT NULL,
            source_column VARCHAR(255) NOT NULL,
            target_dataset_id VARCHAR(255) NOT NULL,
            target_column VARCHAR(255) NOT NULL,
            transformation_logic TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            FOREIGN KEY (transformation_id) REFERENCES lineage_transformations(id),
            FOREIGN KEY (source_dataset_id) REFERENCES lineage_datasets(id),
            FOREIGN KEY (target_dataset_id) REFERENCES lineage_datasets(id)
        );

        -- Indexes for performance
        CREATE INDEX IF NOT EXISTS idx_datasets_name ON lineage_datasets(name);
        CREATE INDEX IF NOT EXISTS idx_datasets_type ON lineage_datasets(type);
        CREATE INDEX IF NOT EXISTS idx_datasets_created ON lineage_datasets(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_transformations_name ON lineage_transformations(name);
        CREATE INDEX IF NOT EXISTS idx_transformations_executed ON lineage_transformations(executed_at DESC);
        CREATE INDEX IF NOT EXISTS idx_edges_source ON lineage_edges(source_dataset_id);
        CREATE INDEX IF NOT EXISTS idx_edges_target ON lineage_edges(target_dataset_id);
        """

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_tables_sql)
                conn.commit()
            logger.info("Lineage tracking schema initialized")
        except Exception as e:
            logger.error(f"Error initializing lineage schema: {e}")

    def register_dataset(self, dataset: Dataset) -> str:
        """Register a new dataset in the lineage system"""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO lineage_datasets
                        (id, name, type, schema, location, row_count, created_at, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            row_count = EXCLUDED.row_count,
                            metadata = EXCLUDED.metadata
                        RETURNING id
                    """,
                        (
                            dataset.id,
                            dataset.name,
                            dataset.type.value,
                            Json(dataset.schema),
                            dataset.location,
                            dataset.row_count,
                            dataset.created_at,
                            Json(dataset.metadata),
                        ),
                    )
                    result = cur.fetchone()
                conn.commit()

            logger.info(f"Registered dataset: {dataset.name} (ID: {dataset.id})")
            return result[0]

        except Exception as e:
            logger.error(f"Error registering dataset: {e}")
            raise

    def track_transformation(self, transformation: Transformation) -> str:
        """Track a transformation between datasets"""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Insert transformation
                    cur.execute(
                        """
                        INSERT INTO lineage_transformations
                        (id, name, type, description, input_datasets, output_dataset,
                         code_reference, parameters, executed_at, duration_seconds,
                         row_count_input, row_count_output, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """,
                        (
                            transformation.id,
                            transformation.name,
                            transformation.type.value,
                            transformation.description,
                            Json(transformation.input_datasets),
                            transformation.output_dataset,
                            transformation.code_reference,
                            Json(transformation.parameters),
                            transformation.executed_at,
                            transformation.duration_seconds,
                            transformation.row_count_input,
                            transformation.row_count_output,
                            transformation.status,
                        ),
                    )

                    # Create lineage edges
                    for input_dataset in transformation.input_datasets:
                        cur.execute(
                            """
                            INSERT INTO lineage_edges
                            (source_dataset_id, target_dataset_id, transformation_id)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (source_dataset_id, target_dataset_id, transformation_id)
                            DO NOTHING
                        """,
                            (input_dataset, transformation.output_dataset, transformation.id),
                        )

                conn.commit()

            logger.info(f"Tracked transformation: {transformation.name}")
            return transformation.id

        except Exception as e:
            logger.error(f"Error tracking transformation: {e}")
            raise

    def track_column_lineage(
        self,
        transformation_id: str,
        source_dataset_id: str,
        source_column: str,
        target_dataset_id: str,
        target_column: str,
        transformation_logic: str,
    ):
        """Track column-level lineage"""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO lineage_columns
                        (transformation_id, source_dataset_id, source_column,
                         target_dataset_id, target_column, transformation_logic)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                        (
                            transformation_id,
                            source_dataset_id,
                            source_column,
                            target_dataset_id,
                            target_column,
                            transformation_logic,
                        ),
                    )
                conn.commit()

        except Exception as e:
            logger.error(f"Error tracking column lineage: {e}")

    def get_dataset_lineage(self, dataset_id: str, direction: str = "both") -> Dict:
        """
        Get lineage for a dataset
        direction: 'upstream', 'downstream', or 'both'
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    lineage = {
                        "dataset": None,
                        "upstream": [],
                        "downstream": [],
                        "transformations": [],
                    }

                    # Get dataset info
                    cur.execute(
                        """
                        SELECT * FROM lineage_datasets WHERE id = %s
                    """,
                        (dataset_id,),
                    )
                    lineage["dataset"] = cur.fetchone()

                    if direction in ["upstream", "both"]:
                        # Get upstream datasets
                        cur.execute(
                            """
                            SELECT
                                d.*,
                                t.name as transformation_name,
                                t.type as transformation_type
                            FROM lineage_datasets d
                            JOIN lineage_edges e ON d.id = e.source_dataset_id
                            JOIN lineage_transformations t ON e.transformation_id = t.id
                            WHERE e.target_dataset_id = %s
                        """,
                            (dataset_id,),
                        )
                        lineage["upstream"] = cur.fetchall()

                    if direction in ["downstream", "both"]:
                        # Get downstream datasets
                        cur.execute(
                            """
                            SELECT
                                d.*,
                                t.name as transformation_name,
                                t.type as transformation_type
                            FROM lineage_datasets d
                            JOIN lineage_edges e ON d.id = e.target_dataset_id
                            JOIN lineage_transformations t ON e.transformation_id = t.id
                            WHERE e.source_dataset_id = %s
                        """,
                            (dataset_id,),
                        )
                        lineage["downstream"] = cur.fetchall()

                    # Get related transformations
                    cur.execute(
                        """
                        SELECT * FROM lineage_transformations
                        WHERE output_dataset = %s
                           OR %s = ANY(
                               SELECT jsonb_array_elements_text(input_datasets)
                           )
                        ORDER BY executed_at DESC
                    """,
                        (dataset_id, dataset_id),
                    )
                    lineage["transformations"] = cur.fetchall()

            return lineage

        except Exception as e:
            logger.error(f"Error getting dataset lineage: {e}")
            return {}

    def get_transformation_history(self, dataset_name: str, limit: int = 10) -> List[Dict]:
        """Get transformation history for a dataset"""
        try:
            with self._get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT
                            t.*,
                            d.name as output_dataset_name
                        FROM lineage_transformations t
                        JOIN lineage_datasets d ON t.output_dataset = d.id
                        WHERE d.name = %s
                        ORDER BY t.executed_at DESC
                        LIMIT %s
                    """,
                        (dataset_name, limit),
                    )
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting transformation history: {e}")
            return []

    def generate_lineage_graph(self, dataset_id: str) -> Dict:
        """Generate a graph representation of data lineage"""
        try:
            with self._get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Get all related nodes and edges
                    cur.execute(
                        """
                        WITH RECURSIVE lineage_tree AS (
                            -- Base case: start dataset
                            SELECT id, name, type, 0 as level
                            FROM lineage_datasets
                            WHERE id = %s

                            UNION

                            -- Recursive case: upstream
                            SELECT d.id, d.name, d.type, lt.level - 1
                            FROM lineage_datasets d
                            JOIN lineage_edges e ON d.id = e.source_dataset_id
                            JOIN lineage_tree lt ON e.target_dataset_id = lt.id
                            WHERE lt.level > -5  -- Limit depth

                            UNION

                            -- Recursive case: downstream
                            SELECT d.id, d.name, d.type, lt.level + 1
                            FROM lineage_datasets d
                            JOIN lineage_edges e ON d.id = e.target_dataset_id
                            JOIN lineage_tree lt ON e.source_dataset_id = lt.id
                            WHERE lt.level < 5  -- Limit depth
                        )
                        SELECT DISTINCT id, name, type, level
                        FROM lineage_tree
                        ORDER BY level, name
                    """,
                        (dataset_id,),
                    )
                    nodes = cur.fetchall()

                    # Get edges between these nodes
                    node_ids = [n["id"] for n in nodes]
                    cur.execute(
                        """
                        SELECT
                            e.*,
                            t.name as transformation_name,
                            t.type as transformation_type
                        FROM lineage_edges e
                        JOIN lineage_transformations t ON e.transformation_id = t.id
                        WHERE e.source_dataset_id = ANY(%s)
                          AND e.target_dataset_id = ANY(%s)
                    """,
                        (node_ids, node_ids),
                    )
                    edges = cur.fetchall()

            return {"nodes": nodes, "edges": edges}

        except Exception as e:
            logger.error(f"Error generating lineage graph: {e}")
            return {"nodes": [], "edges": []}

    def get_data_quality_impact(self, dataset_id: str) -> Dict:
        """Analyze impact of data quality issues on downstream datasets"""
        try:
            with self._get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        WITH RECURSIVE downstream AS (
                            SELECT id, name, 1 as depth
                            FROM lineage_datasets
                            WHERE id = %s

                            UNION

                            SELECT d.id, d.name, ds.depth + 1
                            FROM lineage_datasets d
                            JOIN lineage_edges e ON d.id = e.target_dataset_id
                            JOIN downstream ds ON e.source_dataset_id = ds.id
                            WHERE ds.depth < 10
                        )
                        SELECT
                            name,
                            COUNT(*) as affected_count,
                            MAX(depth) as max_depth
                        FROM downstream
                        WHERE depth > 1
                        GROUP BY name
                    """,
                        (dataset_id,),
                    )

                    return {"source_dataset": dataset_id, "affected_datasets": cur.fetchall()}
        except Exception as e:
            logger.error(f"Error analyzing quality impact: {e}")
            return {}


def generate_dataset_id(name: str, timestamp: datetime) -> str:
    """Generate unique dataset ID"""
    content = f"{name}_{timestamp.isoformat()}"
    return hashlib.md5(content.encode()).hexdigest()


def generate_transformation_id(name: str, timestamp: datetime) -> str:
    """Generate unique transformation ID"""
    content = f"{name}_{timestamp.isoformat()}"
    return hashlib.md5(content.encode()).hexdigest()


# Example usage
if __name__ == "__main__":
    tracker = DataLineageTracker()

    # Example: Track stock data ingestion
    now = datetime.now()

    # Register raw dataset
    raw_dataset = Dataset(
        id=generate_dataset_id("stock_prices_raw", now),
        name="stock_prices_raw",
        type=DatasetType.RAW,
        schema={
            "symbol": "VARCHAR",
            "timestamp": "TIMESTAMP",
            "close_price": "DECIMAL",
            "volume": "BIGINT",
        },
        location="postgresql://localhost/financial_data/stock_prices_raw",
        row_count=1000,
        created_at=now,
        metadata={"source": "yfinance", "symbols": ["AAPL", "GOOGL"], "quality_score": 0.98},
    )
    tracker.register_dataset(raw_dataset)

    # Register processed dataset
    processed_dataset = Dataset(
        id=generate_dataset_id("stock_metrics_5min", now),
        name="stock_metrics_5min",
        type=DatasetType.AGGREGATED,
        schema={
            "symbol": "VARCHAR",
            "window_start": "TIMESTAMP",
            "avg_price": "DECIMAL",
            "volume": "BIGINT",
        },
        location="postgresql://localhost/financial_data/stock_metrics_5min",
        row_count=50,
        created_at=now,
        metadata={"aggregation_window": "5min"},
    )
    tracker.register_dataset(processed_dataset)

    # Track transformation
    transformation = Transformation(
        id=generate_transformation_id("5min_aggregation", now),
        name="5min_aggregation",
        type=TransformationType.AGGREGATION,
        description="Calculate 5-minute windowed aggregations",
        input_datasets=[raw_dataset.id],
        output_dataset=processed_dataset.id,
        code_reference="src/processing/spark_streaming.py:_calculate_5min_metrics",
        parameters={"window": "5 minutes", "slide": "1 minute"},
        executed_at=now,
        duration_seconds=15.5,
        row_count_input=1000,
        row_count_output=50,
        status="success",
    )
    tracker.track_transformation(transformation)

    # Track column-level lineage
    tracker.track_column_lineage(
        transformation_id=transformation.id,
        source_dataset_id=raw_dataset.id,
        source_column="close_price",
        target_dataset_id=processed_dataset.id,
        target_column="avg_price",
        transformation_logic="AVG(close_price) OVER (window)",
    )

    # Query lineage
    lineage = tracker.get_dataset_lineage(processed_dataset.id)
    logger.info(f"Lineage: {json.dumps(lineage, indent=2, default=str)}")
