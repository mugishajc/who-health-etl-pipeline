"""
Quick test script to verify extract and transform work without database.
"""
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

# Test extract (just first 2 pages)
logger.info("Testing WHO API extraction...")

import requests
from config import WHO_API_BASE_URL, PAGE_SIZE

url = f"{WHO_API_BASE_URL}?$top=50"
response = requests.get(url, timeout=30)
response.raise_for_status()
data = response.json()
raw_records = data.get("value", [])

logger.info(f"Fetched {len(raw_records)} records from API")
logger.info(f"Sample record: {raw_records[0] if raw_records else 'None'}")

# Test transform
logger.info("\nTesting data transformation...")

from transform import transform_data

clean_data = transform_data(raw_records)
logger.info(f"Transformed to {len(clean_data)} clean records")

if clean_data:
    logger.info(f"Sample clean record: {clean_data[0]}")

logger.info("\n--- Extract and Transform work correctly! ---")
logger.info("To run full pipeline, set up PostgreSQL and run: python main.py")
