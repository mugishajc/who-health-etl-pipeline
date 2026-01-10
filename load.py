"""
Database loading module for WHO Health Data ETL Pipeline.

Handles PostgreSQL connections, data loading with upsert logic,
and checkpoint management for pipeline state persistence.
"""

import json
import logging
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import extras

from config import DB_CONFIG, PIPELINE_NAME

logger = logging.getLogger(__name__)


def get_db_connection():
    """
    Create and return a PostgreSQL database connection.

    Returns:
        psycopg2 connection object

    Raises:
        psycopg2.Error: If connection fails
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise


def load_data(data: List[Dict[str, Any]]) -> int:
    """
    Load transformed data into PostgreSQL using upsert.

    Args:
        data: List of dictionaries containing cleaned records

    Returns:
        Number of records processed
    """
    if not data:
        logger.warning("No data to load")
        return 0

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Upsert query - update value and fetched_at on conflict
        upsert_sql = """
            INSERT INTO health_indicators
                (country_code, indicator_code, indicator_name, year, value, source_url)
            VALUES
                (%(country_code)s, %(indicator_code)s, %(indicator_name)s,
                 %(year)s, %(value)s, %(source_url)s)
            ON CONFLICT (country_code, indicator_code, year)
            DO UPDATE SET
                value = EXCLUDED.value,
                fetched_at = NOW()
        """

        # Batch insert for performance
        extras.execute_batch(cursor, upsert_sql, data, page_size=100)

        # Update pipeline metadata
        update_metadata_sql = """
            UPDATE pipeline_metadata
            SET records_processed = records_processed + %s,
                updated_at = NOW()
            WHERE pipeline_name = %s
        """
        cursor.execute(update_metadata_sql, (len(data), PIPELINE_NAME))

        conn.commit()
        logger.info(f"Successfully loaded {len(data)} records")
        return len(data)

    except psycopg2.Error as e:
        logger.error(f"Database load failed: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def get_last_checkpoint() -> Optional[Dict[str, Any]]:
    """
    Retrieve the last checkpoint from pipeline metadata.

    Returns:
        Checkpoint dictionary or None if no checkpoint exists
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT last_checkpoint FROM pipeline_metadata WHERE pipeline_name = %s",
            (PIPELINE_NAME,)
        )

        result = cursor.fetchone()
        if result and result[0]:
            return result[0]  # JSONB is automatically parsed
        return None

    except psycopg2.Error as e:
        logger.error(f"Failed to get checkpoint: {e}")
        return None
    finally:
        if conn:
            conn.close()


def save_checkpoint(
    checkpoint_data: Optional[Dict[str, Any]],
    status: str = "running"
) -> None:
    """
    Save checkpoint to pipeline metadata.

    Args:
        checkpoint_data: Dictionary with checkpoint info (None to clear)
        status: Pipeline status ('running', 'completed', 'failed')
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        checkpoint_json = json.dumps(checkpoint_data) if checkpoint_data else None

        upsert_sql = """
            INSERT INTO pipeline_metadata (pipeline_name, last_checkpoint, status, last_run_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (pipeline_name)
            DO UPDATE SET
                last_checkpoint = EXCLUDED.last_checkpoint,
                status = EXCLUDED.status,
                last_run_at = NOW(),
                updated_at = NOW()
        """

        cursor.execute(upsert_sql, (PIPELINE_NAME, checkpoint_json, status))
        conn.commit()

        logger.debug(f"Checkpoint saved: {checkpoint_data}, status: {status}")

    except psycopg2.Error as e:
        logger.error(f"Failed to save checkpoint: {e}")
    finally:
        if conn:
            conn.close()
