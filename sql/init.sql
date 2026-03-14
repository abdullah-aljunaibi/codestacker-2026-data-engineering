-- Database initialization script
-- This creates the necessary schemas for the data pipeline

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.shipping_spend_by_tier (
    tier TEXT NOT NULL,
    year_month TEXT NOT NULL CHECK (year_month ~ '^\d{4}-\d{2}$'),
    total_shipping_spend DECIMAL(12,2) NOT NULL CHECK (total_shipping_spend >= 0),
    shipment_count INTEGER NOT NULL CHECK (shipment_count > 0),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tier, year_month)
);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA staging TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO airflow;

-- Set default privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO airflow;
