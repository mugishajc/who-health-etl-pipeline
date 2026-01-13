"""Test database with some queries."""
import psycopg2
from config import DB_CONFIG

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("Connected to database successfully!\n")

    # Check tables
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = cursor.fetchall()

    print("Tables in database:")
    for table in tables:
        print(f"  - {table[0]}")

    print("\n" + "="*50)
    print("Database is ready!")
    print("="*50)

    # Show table structure
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'health_indicators'
        ORDER BY ordinal_position
    """)

    print("\nhealth_indicators table structure:")
    for col in cursor.fetchall():
        print(f"  {col[0]}: {col[1]}")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")
