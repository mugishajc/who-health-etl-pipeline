"""
Main orchestrator for WHO Health Data ETL Pipeline.

Coordinates the Extract, Transform, Load workflow with
checkpoint support for resilient pipeline execution.
"""

import logging
import sys

from extract import extract_data
from transform import transform_data
from load import load_data, get_last_checkpoint, save_checkpoint

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


def run_pipeline():
    """
    Execute the complete ETL pipeline.

    Steps:
    1. Check for existing checkpoint to resume from
    2. Extract data from WHO API
    3. Transform and validate data
    4. Load data into PostgreSQL
    5. Update pipeline status
    """
    logger.info("=" * 50)
    logger.info("WHO Health Data ETL Pipeline - Starting")
    logger.info("=" * 50)

    checkpoint = None

    try:
        # Check for resume point
        checkpoint = get_last_checkpoint()
        if checkpoint:
            logger.info(f"Found checkpoint: {checkpoint}")
        else:
            logger.info("No checkpoint found - starting fresh")

        # Extract
        logger.info("Step 1: Extracting data from WHO API...")
        raw_data = extract_data(start_from=checkpoint)
        logger.info(f"Extraction complete: {len(raw_data)} records")

        # Transform
        logger.info("Step 2: Transforming data...")
        clean_data = transform_data(raw_data)
        logger.info(f"Transformation complete: {len(clean_data)} clean records")

        # Load
        logger.info("Step 3: Loading data into PostgreSQL...")
        records_loaded = load_data(clean_data)
        logger.info(f"Load complete: {records_loaded} records upserted")

        # Mark pipeline as completed
        save_checkpoint(None, status="completed")

        logger.info("=" * 50)
        logger.info("Pipeline completed successfully!")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        save_checkpoint(checkpoint, status="failed")
        raise


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
