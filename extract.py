"""
Data extraction module for WHO Health Data ETL Pipeline.

Handles fetching data from the WHO GHO OData API with retry logic,
pagination, and checkpoint support for resilient data extraction.
"""

import logging
import time
from typing import Any, Dict, List, Optional

import requests

from config import PAGE_SIZE, REQUEST_DELAY, WHO_API_BASE_URL
from load import save_checkpoint
from utils.http import fetch_with_retry

logger = logging.getLogger(__name__)


def extract_data(start_from: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Extract data from WHO GHO API with pagination and checkpoint support.

    Args:
        start_from: Optional checkpoint dictionary with 'page' key to resume from

    Returns:
        List of all fetched records from the API

    Raises:
        requests.RequestException: If API requests fail after retries
    """
    all_records: List[Dict[str, Any]] = []
    page = 0

    # Resume from checkpoint if provided
    if start_from and "page" in start_from:
        page = start_from["page"]
        logger.info(f"Resuming extraction from page {page}")

    while True:
        # Calculate pagination parameters
        skip = page * PAGE_SIZE
        url = f"{WHO_API_BASE_URL}?$skip={skip}&$top={PAGE_SIZE}"

        try:
            logger.info(f"Fetching page {page + 1} (skip={skip}, top={PAGE_SIZE})")
            data = fetch_with_retry(url)

            # Extract records from response
            records = data.get("value", [])
            record_count = len(records)

            logger.info(f"Page {page + 1}: Retrieved {record_count} records")
            all_records.extend(records)

            # Save checkpoint after successful page fetch
            checkpoint = {"page": page + 1}
            save_checkpoint(checkpoint, status="running")

            # Check if we've reached the end of data
            if record_count < PAGE_SIZE:
                logger.info(
                    f"Reached end of data. Last page had {record_count} records "
                    f"(less than PAGE_SIZE={PAGE_SIZE})"
                )
                break

            # Move to next page
            page += 1

            # Rate limiting - be a good API citizen
            time.sleep(REQUEST_DELAY)

        except requests.RequestException as e:
            logger.error(f"Extraction failed at page {page}: {e}")
            # Save checkpoint on failure for later resume
            checkpoint = {"page": page}
            save_checkpoint(checkpoint, status="failed")
            raise

    logger.info(f"Extraction complete. Total records fetched: {len(all_records)}")
    return all_records
