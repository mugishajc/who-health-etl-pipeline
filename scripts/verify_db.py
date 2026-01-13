"""
Quick database verification script.
Run this after setting up PostgreSQL to confirm everything works.
"""

import psycopg2
from config import DB_CONFIG

def check_connection():
    """Test database connection and show what's in the database."""
    try:
        print("Connecting to PostgreSQL...")
        print(f"Host: {DB_CONFIG['host']}")
        print(f"Port: {DB_CONFIG['port']}")
        print(f"Database: {DB_CONFIG['dbname']}")
        print(f"User: {DB_CONFIG['user']}")
        print()

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("✓ Connection successful!")
        print()

        # Check if tables exist
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()

        if tables:
            print("Tables found:")
            for table in tables:
                print(f"  - {table[0]}")
            print()
        else:
            print("No tables found. Run 'python setup_db.py' to create them.")
            print()

        # Check if data exists
        try:
            cursor.execute("SELECT COUNT(*) FROM health_indicators")
            count = cursor.fetchone()[0]
            print(f"Records in health_indicators: {count}")

            if count > 0:
                cursor.execute("""
                    SELECT country_code, year, value
                    FROM health_indicators
                    ORDER BY country_code, year
                    LIMIT 5
                """)
                print("\nSample data:")
                for row in cursor.fetchall():
                    print(f"  {row[0]} | {row[1]} | {row[2]:.1f} years")
            print()

            # Check pipeline status
            cursor.execute("SELECT status, records_processed, last_run_at FROM pipeline_metadata WHERE pipeline_name = 'who_etl'")
            result = cursor.fetchone()
            if result:
                status, records, last_run = result
                print(f"Pipeline status: {status}")
                print(f"Records processed: {records}")
                print(f"Last run: {last_run}")

        except psycopg2.Error:
            print("Tables exist but may be empty. Run 'python main.py' to load data.")

        conn.close()

    except psycopg2.OperationalError as e:
        print("✗ Connection failed!")
        print(f"\nError: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure PostgreSQL is running")
        print("2. Check your .env file has the correct credentials")
        print("3. Verify the database exists: psql -U postgres -c '\\l'")
    except Exception as e:
        print(f"✗ Unexpected error: {e}")

if __name__ == "__main__":
    check_connection()
