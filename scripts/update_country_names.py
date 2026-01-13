"""
Update country names in the database based on country codes.
Fetches country metadata from WHO API.
"""

import psycopg2
import requests
import sys
sys.path.insert(0, '..')
from config import DB_CONFIG

def fetch_country_names():
    """Fetch country/region names from WHO API."""
    url = "https://ghoapi.azureedge.net/api/DIMENSION/COUNTRY/DimensionValues"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        mapping = {}
        for item in data.get('value', []):
            code = item.get('Code')
            title = item.get('Title')
            if code and title:
                mapping[code] = title

        return mapping
    except Exception as e:
        print(f"Failed to fetch country names: {e}")
        return {}

def update_database(mapping):
    """Update country_name column in database."""
    if not mapping:
        print("No country mapping available")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    updated = 0
    for code, name in mapping.items():
        cursor.execute(
            "UPDATE health_indicators SET country_name = %s WHERE country_code = %s",
            (name, code)
        )
        updated += cursor.rowcount

    conn.commit()
    print(f"Updated {updated} records with country names")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    print("Fetching country names from WHO API...")
    mapping = fetch_country_names()
    print(f"Found {len(mapping)} country mappings")

    print("Updating database...")
    update_database(mapping)
    print("Done!")
