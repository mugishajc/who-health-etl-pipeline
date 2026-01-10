-- WHO Health Data ETL Pipeline Database Schema
-- PostgreSQL 12+

-- Table to store health indicator data from WHO GHO API
CREATE TABLE IF NOT EXISTS health_indicators (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(10) NOT NULL,
    country_name VARCHAR(200),
    indicator_code VARCHAR(50) NOT NULL,
    indicator_name VARCHAR(200),
    year INT NOT NULL,
    value FLOAT NOT NULL,
    source_url TEXT,
    fetched_at TIMESTAMP DEFAULT NOW(),

    -- Ensure no duplicate records for same country/indicator/year combination
    CONSTRAINT unique_country_indicator_year UNIQUE (country_code, indicator_code, year)
);

-- Index for common query patterns
CREATE INDEX IF NOT EXISTS idx_health_indicators_country ON health_indicators(country_code);
CREATE INDEX IF NOT EXISTS idx_health_indicators_year ON health_indicators(year);
CREATE INDEX IF NOT EXISTS idx_health_indicators_indicator ON health_indicators(indicator_code);

-- Table to track pipeline execution state and checkpoints
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) UNIQUE NOT NULL,
    last_run_at TIMESTAMP,
    last_checkpoint JSONB,
    status VARCHAR(50),
    records_processed INT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Insert initial pipeline record
INSERT INTO pipeline_metadata (pipeline_name, status)
VALUES ('who_etl', 'initialized')
ON CONFLICT (pipeline_name) DO NOTHING;
