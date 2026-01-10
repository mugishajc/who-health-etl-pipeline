# WHO Health Data ETL Pipeline

ETL pipeline that extracts life expectancy data from WHO's GHO API and loads it into PostgreSQL.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Test without database (recommended first step)
python test_pipeline.py

# Set up database
createdb who_etl
python setup_db.py

# Configure connection (optional - defaults work for local postgres)
cp .env.example .env

# Run the pipeline
python main.py
```

## Project Structure

```
├── config.py              # Configuration and environment variables
├── extract.py             # API data extraction with pagination
├── transform.py           # Data validation and cleaning
├── load.py               # PostgreSQL loading with upserts
├── main.py               # Pipeline orchestrator
├── schema.sql            # Database schema
├── setup_db.py           # Database setup script
├── test_pipeline.py      # Quick smoke test
├── run_tests.py          # Test runner
├── utils/                # Reusable utility modules
│   ├── http.py          # HTTP retry logic
│   ├── checkpoint.py    # Checkpoint management class
│   └── validation.py    # Data validation helpers
└── tests/
    ├── test_extract.py      # Unit tests for extraction
    ├── test_transform.py    # Unit tests for transformation
    └── test_integration.py  # Integration tests
```

## Design Decisions

**Data Source**: Chose life expectancy indicator (WHOSIS_000001) for its clean structure and global coverage.

**Checkpoint/Resume**: Pipeline saves progress after each API page. If it crashes, rerun to resume from last checkpoint instead of starting over.

**Idempotency**: Uses PostgreSQL upserts (ON CONFLICT) so reruns update existing data rather than creating duplicates.

**Retry Logic**: Exponential backoff for transient API failures (network issues, rate limits).

## Data Validation

Transform step removes:
- Null country codes or years
- Years outside 1900-2030 range
- Negative life expectancy values
- Duplicate country/year records

## Testing

Run unit tests:
```bash
python run_tests.py
```

Test extract/transform without database:
```bash
python test_pipeline.py
```

All tests use mocks where needed and validate:
- API retry logic with exponential backoff
- Pagination and checkpoint resume
- Data validation rules
- Type conversions
- End-to-end data flow

## Improvements for Production

- Incremental loads (fetch only new data since last run)
- Multiple indicator support
- Alerting on failures
- Data quality metrics
- More granular checkpointing

## Verifying Results

```sql
-- Sample the loaded data
SELECT country_code, year, value
FROM health_indicators
ORDER BY country_code, year
LIMIT 20;

-- Check pipeline status
SELECT pipeline_name, status, records_processed, last_run_at
FROM pipeline_metadata;
```
