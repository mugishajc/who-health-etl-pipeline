"""Create the database if it doesn't exist."""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Connect to postgres database to create our database
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="Delasoft@123@",
    dbname="postgres"
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cursor = conn.cursor()

# Check if database exists
cursor.execute("SELECT 1 FROM pg_database WHERE datname='who_etl'")
exists = cursor.fetchone()

if not exists:
    cursor.execute("CREATE DATABASE who_etl")
    print("Database 'who_etl' created successfully!")
else:
    print("Database 'who_etl' already exists.")

cursor.close()
conn.close()
