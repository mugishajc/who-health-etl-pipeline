"""
Configuration module for WHO Health Data ETL Pipeline.

Loads environment variables and defines application constants.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# WHO GHO API Configuration
WHO_API_BASE_URL = "https://ghoapi.azureedge.net/api/WHOSIS_000001"
INDICATOR_CODE = "WHOSIS_000001"
PAGE_SIZE = 100

# Database Configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "who_etl"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Pipeline Configuration
PIPELINE_NAME = "who_etl"
REQUEST_DELAY = 0.5  # Delay between API requests in seconds
