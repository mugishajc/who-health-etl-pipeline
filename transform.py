"""
Data transformation module for WHO Health Data ETL Pipeline.

Handles cleaning, validation, and transformation of raw WHO API data
into a structured format suitable for database loading.
"""

import logging
from typing import Any, Dict, List

import pandas as pd

from config import INDICATOR_CODE

logger = logging.getLogger(__name__)

# Constants for this indicator
INDICATOR_NAME = "Life expectancy at birth (years)"
SOURCE_URL = "https://ghoapi.azureedge.net/api"

# Validation constraints
MIN_YEAR = 1900
MAX_YEAR = 2030
MIN_VALUE = 0


def transform_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform and validate raw WHO API data.

    Performs the following transformations:
    - Selects and renames relevant columns
    - Validates data types and value ranges
    - Removes null values and duplicates
    - Adds metadata columns

    Args:
        raw_data: List of dictionaries from WHO API response

    Returns:
        List of cleaned and validated dictionaries ready for database loading
    """
    if not raw_data:
        logger.warning("No data to transform - received empty input")
        return []

    initial_count = len(raw_data)
    logger.info(f"Starting transformation of {initial_count} records")

    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(raw_data)

    # Check required columns exist
    required_columns = ["SpatialDim", "TimeDim", "NumericValue"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return []

    # Select and rename columns
    column_mapping = {
        "SpatialDim": "country_code",
        "TimeDim": "year",
        "NumericValue": "value",
    }

    # Include Dim1 if present (optional dimension)
    if "Dim1" in df.columns:
        column_mapping["Dim1"] = "dimension"

    df = df[list(column_mapping.keys())].rename(columns=column_mapping)

    # Track validation statistics
    stats = {"initial": len(df)}

    # Remove rows with null values in critical columns
    df = df.dropna(subset=["country_code", "year", "value"])
    stats["after_null_removal"] = len(df)
    logger.info(
        f"Removed {stats['initial'] - stats['after_null_removal']} records "
        "with null country_code, year, or value"
    )

    # Convert types
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Remove rows that failed type conversion
    df = df.dropna(subset=["year", "value"])
    stats["after_type_conversion"] = len(df)

    # Validate year range
    year_mask = (df["year"] >= MIN_YEAR) & (df["year"] <= MAX_YEAR)
    invalid_years = len(df[~year_mask])
    df = df[year_mask]
    stats["after_year_filter"] = len(df)

    if invalid_years > 0:
        logger.info(
            f"Removed {invalid_years} records with year outside "
            f"valid range ({MIN_YEAR}-{MAX_YEAR})"
        )

    # Validate value range (non-negative)
    value_mask = df["value"] >= MIN_VALUE
    invalid_values = len(df[~value_mask])
    df = df[value_mask]
    stats["after_value_filter"] = len(df)

    if invalid_values > 0:
        logger.info(f"Removed {invalid_values} records with negative values")

    # Remove duplicates, keeping first occurrence
    df = df.drop_duplicates(subset=["country_code", "year"], keep="first")
    stats["after_dedup"] = len(df)

    duplicates_removed = stats["after_value_filter"] - stats["after_dedup"]
    if duplicates_removed > 0:
        logger.info(
            f"Removed {duplicates_removed} duplicate records "
            "(same country_code and year)"
        )

    # Add metadata columns
    df["indicator_code"] = INDICATOR_CODE
    df["indicator_name"] = INDICATOR_NAME
    df["source_url"] = SOURCE_URL

    # Convert year to regular int for database compatibility
    df["year"] = df["year"].astype(int)

    # Log transformation summary
    total_removed = initial_count - len(df)
    logger.info(
        f"Transformation complete. {len(df)} records retained, "
        f"{total_removed} records removed ({total_removed/initial_count*100:.1f}%)"
    )

    # Convert to list of dictionaries
    result = df.to_dict(orient="records")

    return result
