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

# Verify database connection
python verify_db.py

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
├── verify_db.py          # Database connection checker
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

## Testing and Debugging

### Option 1: Quick Smoke Test (No Database Required)

Tests extraction and transformation with real WHO API data:

```bash
python test_pipeline.py
```

**What it does:**
- Fetches 50 records from WHO API
- Runs full transformation pipeline
- Shows sample input/output
- Verifies data structure

**Use this to:** Quickly verify the pipeline works without database setup.

### Option 2: Full Unit Test Suite (No Database Required)

Runs all 14 unit and integration tests:

```bash
python run_tests.py
```

**What it tests:**
- **Extraction (5 tests)**
  - HTTP retry with exponential backoff
  - Pagination stops at incomplete pages
  - Resume from checkpoint functionality
  - Maximum retries exhaustion handling
- **Transformation (7 tests)**
  - Empty input handling
  - Null value removal
  - Year range validation (1900-2030)
  - Negative value filtering
  - Duplicate record removal
  - Type conversions
  - Basic transformation correctness
- **Integration (2 tests)**
  - End-to-end extract → transform flow
  - Data structure compatibility between stages

**Output:** 14 tests passing, ~0.05s runtime

### Option 3: Full Pipeline Test (Database Required)

Complete ETL pipeline with PostgreSQL:

```bash
# Setup database
createdb who_etl
python setup_db.py

# Run pipeline
python main.py
```

**What it does:**
- Extracts all pages from WHO API (paginated)
- Transforms and validates data
- Loads into PostgreSQL with upserts
- Saves checkpoints for resume capability

**Verify results:**
```sql
-- Count loaded records
SELECT COUNT(*) FROM health_indicators;

-- Sample data
SELECT country_code, year, value
FROM health_indicators
LIMIT 10;
```

### Debugging Tips

**Check logs:** Pipeline outputs detailed logging at INFO level

**Resume after failure:** If pipeline crashes, just run `python main.py` again - it resumes from last checkpoint

**Clear checkpoint:** Delete checkpoint to start fresh:
```sql
UPDATE pipeline_metadata SET last_checkpoint = NULL WHERE pipeline_name = 'who_etl';
```

## What I'd Add for Production

- Incremental loads - only fetch new data since last run instead of everything
- Support for multiple health indicators, not just life expectancy
- Alerts when the pipeline fails (Slack, email, PagerDuty)
- Data quality checks - track completeness, outliers, unexpected patterns
- More detailed checkpoints if processing really large datasets

## Design Trade-offs

I picked life expectancy as the indicator because the data is clean and easy to verify. Could expand to multiple indicators, but wanted to keep things focused for now.

The pipeline does a full refresh each time instead of incremental loads. Full refresh is simpler - you don't need to track what changed since the last run. For ~50K records, it runs fast enough that the complexity of incremental loading isn't worth it yet. In production with larger datasets, I'd add incremental loading to save time.

I went with pandas for data transformation. It makes type conversions and handling missing values straightforward. The memory overhead isn't a concern for datasets under 1GB, but if we were processing much larger files, I'd reconsider.

Checkpoints save after each page (100 records) instead of after every single record. This balances the ability to resume if something crashes with not hammering the database with constant writes. Saving after every record would slow things down unnecessarily.

For the upsert strategy, I used PostgreSQL's ON CONFLICT instead of deleting and reinserting. This preserves the fetched_at timestamp and is safer if multiple processes access the data. The SQL is a bit more complex, but the benefits are worth it.

The pipeline runs synchronously - one page at a time. I could fetch multiple pages in parallel, but that adds complexity and might hit API rate limits. For the data volume here, single-threaded is plenty fast and easier to debug.

No alerting or monitoring hooks yet. In production, I'd integrate with something like PagerDuty or Slack to get notified when runs fail.

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
