"""
Checkpoint management utilities.
Reusable for any pipeline that needs state persistence.
"""
import json
import logging
from typing import Any, Dict, Optional

import psycopg2

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Manages pipeline checkpoints in PostgreSQL."""

    def __init__(self, db_config: Dict[str, Any], pipeline_name: str):
        self.db_config = db_config
        self.pipeline_name = pipeline_name

    def _get_connection(self):
        """Create database connection."""
        try:
            return psycopg2.connect(**self.db_config)
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def get(self) -> Optional[Dict[str, Any]]:
        """Retrieve the last checkpoint."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(
                "SELECT last_checkpoint FROM pipeline_metadata WHERE pipeline_name = %s",
                (self.pipeline_name,)
            )

            result = cursor.fetchone()
            if result and result[0]:
                return result[0]
            return None

        except psycopg2.Error as e:
            logger.error(f"Failed to get checkpoint: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def save(self, checkpoint_data: Optional[Dict[str, Any]], status: str = "running") -> None:
        """Save checkpoint with status."""
        conn = None
        try:
            conn = self._get_connection()
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

            cursor.execute(upsert_sql, (self.pipeline_name, checkpoint_json, status))
            conn.commit()

            logger.debug(f"Checkpoint saved: {checkpoint_data}, status: {status}")

        except psycopg2.Error as e:
            logger.error(f"Failed to save checkpoint: {e}")
        finally:
            if conn:
                conn.close()
