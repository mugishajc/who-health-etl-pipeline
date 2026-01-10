"""
Database setup script.
Connects to PostgreSQL and creates required tables.
"""
import sys
import psycopg2
from config import DB_CONFIG

def setup_database():
    """Create database tables from schema.sql."""
    print("Setting up database...")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Read and execute schema
        with open('schema.sql', 'r') as f:
            schema_sql = f.read()

        cursor.execute(schema_sql)
        conn.commit()

        print("✓ Database tables created successfully")
        print("✓ Initial pipeline metadata inserted")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Database setup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    setup_database()
