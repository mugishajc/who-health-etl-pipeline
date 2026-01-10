"""
HTTP utilities for API interactions.
Reusable across different data sources.
"""
import logging
import time
from typing import Any, Dict

import requests

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
            wait_time = 2 ** attempt
            logger.warning(
                f"Request failed (attempt {attempt + 1}/{max_retries}): {e}. "
                f"Retrying in {wait_time}s..."
            )

            if attempt < max_retries - 1:
                time.sleep(wait_time)

    logger.error(f"All {max_retries} retry attempts failed for URL: {url}")
    raise last_exception
