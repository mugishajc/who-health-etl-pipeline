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

logger = logging.getLogger(__name__)


def fetch_with_retry(
    url: str,
    max_retries: int = 3,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    Make HTTP GET request with exponential backoff retry logic.

    Args:
        url: The URL to fetch
        max_retries: Maximum number of retry attempts
        timeout: Request timeout in seconds

    Returns:
        JSON response as dictionary

    Raises:
        requests.RequestException: After all retries are exhausted
    """
    last_exception = None

    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching URL (attempt {attempt + 1}/{max_retries}): {url}")
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            last_exception = e
            wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
            logger.warning(
                f"Request failed (attempt {attempt + 1}/{max_retries}): {e}. "
                f"Retrying in {wait_time}s..."
            )

            if attempt < max_retries - 1:
                time.sleep(wait_time)

    logger.error(f"All {max_retries} retry attempts failed for URL: {url}")
    raise last_exception


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
