"""
Data validation utilities.
Reusable validation functions for different data types.
"""
from typing import Any


def is_valid_year(year: Any, min_year: int = 1900, max_year: int = 2030) -> bool:
    """Check if year is within valid range."""
    try:
        year_int = int(year)
        return min_year <= year_int <= max_year
    except (ValueError, TypeError):
        return False


def is_positive_number(value: Any) -> bool:
    """Check if value is a positive number."""
    try:
        num = float(value)
        return num >= 0
    except (ValueError, TypeError):
        return False


def has_required_fields(record: dict, required: list) -> bool:
    """Check if record has all required fields with non-null values."""
    return all(
        field in record and record[field] is not None
        for field in required
    )
