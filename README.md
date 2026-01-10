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

## Improvements for Production

- Incremental loads (fetch only new data since last run)
- Multiple indicator support
- Alerting on failures
- Data quality metrics
- More granular checkpointing

## Design Trade-offs

**Full refresh vs incremental**: Chose full data reload for simplicity and correctness. Incremental adds complexity around change detection and requires tracking last sync timestamps. For a 3-hour exercise with ~50K records, full refresh is acceptable.

**Pandas vs manual parsing**: Used pandas for transformations because it handles type conversions and missing data cleanly. Trade-off is memory overhead, but acceptable for datasets under 1GB.

**Checkpoint granularity**: Checkpoint per page (100 records) rather than per record. Balances resume capability with database writes. Finer granularity would slow down the pipeline.

**Single indicator focus**: Extracted only life expectancy data instead of all WHO indicators. Keeps the solution focused and demonstrates the pattern without over-engineering.

**Upsert strategy**: Used ON CONFLICT for idempotency instead of delete-then-insert. Preserves fetched_at history and is safer for concurrent access, though slightly more complex SQL.

**Synchronous execution**: Single-threaded pipeline instead of parallel page fetching. Simpler to reason about, respects API rate limits, and sufficient for the data volume.

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
