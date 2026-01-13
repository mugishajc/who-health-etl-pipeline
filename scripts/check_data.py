"""Check data loaded in the database."""
import psycopg2
from config import DB_CONFIG

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Count records
cursor.execute("SELECT COUNT(*) FROM health_indicators")
count = cursor.fetchone()[0]
print(f"Total records in health_indicators: {count}")

if count > 0:
    # Show sample data
    cursor.execute("""
        SELECT country_code, country_name, year, value
        FROM health_indicators
        ORDER BY country_code, year
        LIMIT 10
    """)

    print("\nSample data:")
    print("-" * 60)
    for row in cursor.fetchall():
        country_name = row[1] if row[1] else "Unknown"
        print(f"{row[0]:4} | {country_name:30} | {row[2]} | {row[3]:.1f} years")

    # Check pipeline status
    print("\n" + "=" * 60)
    cursor.execute("""
        SELECT pipeline_name, status, records_processed, last_run_at
        FROM pipeline_metadata
    """)
    result = cursor.fetchone()
    if result:
        print(f"Pipeline: {result[0]}")
        print(f"Status: {result[1]}")
        print(f"Records processed: {result[2]}")
        print(f"Last run: {result[3]}")
else:
    print("\nNo data yet. Pipeline may still be running.")
    print("Check with: python main.py")

cursor.close()
conn.close()
