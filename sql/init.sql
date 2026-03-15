-- Database initialization script
-- This creates the necessary schemas for the data pipeline

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS analytics.shipping_spend_by_tier (
    tier TEXT NOT NULL,
    year_month TEXT NOT NULL CHECK (year_month ~ '^\d{4}-\d{2}$'),
    total_shipping_spend DECIMAL(12,2) NOT NULL CHECK (total_shipping_spend >= 0),
    shipment_count INTEGER NOT NULL CHECK (shipment_count > 0),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tier, year_month)
);

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    pipeline_run_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS ops.stage_runs (
    id SERIAL PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
    stage_name TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    rows_read INTEGER,
    rows_written INTEGER,
    rows_rejected INTEGER,
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA staging TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA ops TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ops TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ops TO airflow;

-- Set default privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT ALL ON SEQUENCES TO airflow;
