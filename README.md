# WHO Health Data ETL Pipeline

Simple ETL pipeline that pulls life expectancy data from WHO's API and loads it into Postgres.

## Setup

You'll need Python 3.8+ and PostgreSQL running locally.

```bash
# install dependencies
pip install -r requirements.txt

# create the database
psql -U postgres -c "CREATE DATABASE who_etl;"
psql -U postgres -d who_etl -f schema.sql

# configure your db connection
cp .env.example .env
# then edit .env with your postgres password

# run it
python main.py
```

## Project structure

- `extract.py` - pulls data from WHO API, handles pagination and retries
- `transform.py` - cleans up the raw data, filters out bad records
- `load.py` - inserts into postgres, handles duplicates with upsert
- `main.py` - runs the whole thing
- `config.py` - loads env vars
- `schema.sql` - database tables

## Why I built it this way

I picked life expectancy data (WHOSIS_000001) because it's one of the cleaner datasets in the WHO API - good global coverage, consistent structure, easy to verify.

The pipeline saves checkpoints after each page of data it fetches. So if the API times out or something crashes, you can just run it again and it picks up where it left off instead of starting over.

For loading, I use postgres upsert (ON CONFLICT) so running the pipeline twice doesn't create duplicates - it just updates existing records.

## Data validation

The transform step filters out:
- missing country codes or years (unusable)
- years before 1900 or after 2030 (probably data errors)
- negative values (doesn't make sense for life expectancy)
- duplicate country/year combos

## Testing

I tested this manually - ran it a few times, checked the data in postgres, killed it mid-run to make sure resume works. Didn't write unit tests due to time but the transform logic would be easy to test with some sample data.

## What I'd do with more time

- only fetch new data instead of everything each time
- add some actual tests
- maybe pull multiple indicators, not just life expectancy
- better error notifications

## Checking the results

```sql
-- see what got loaded
SELECT country_code, year, value
FROM health_indicators
ORDER BY country_code, year
LIMIT 20;

-- check pipeline status
SELECT * FROM pipeline_metadata;
```
