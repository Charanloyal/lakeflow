-- ==============================================================================
-- PostgreSQL Initialization for data-platform-lab
-- Prepares databases and user permissions for CDC (Debezium) & Airflow metadata
-- ==============================================================================

-- Create Airflow metadata database
CREATE DATABASE airflow;

-- Grant privileges on airflow database
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;

-- Create application schema and sample CDC table in platform_db
\c platform_db;

CREATE SCHEMA IF NOT EXISTS platform;

-- Grant replication permissions (Debezium user)
CREATE ROLE debezium_user WITH REPLICATION LOGIN PASSWORD 'debezium_password';
GRANT ALL PRIVILEGES ON DATABASE platform_db TO debezium_user;
GRANT ALL ON SCHEMA platform TO debezium_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA platform GRANT ALL ON TABLES TO debezium_user;

-- Create sample change data capture table
CREATE TABLE IF NOT EXISTS platform.telemetry_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vehicle_vin VARCHAR(32) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    speed_kph NUMERIC(5, 2),
    battery_level_pct NUMERIC(5, 2),
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Set publication identity to full so Debezium captures old & new values
ALTER TABLE platform.telemetry_events REPLICA IDENTITY FULL;

-- Create publication for Debezium CDC
CREATE PUBLICATION platform_cdc_publication FOR TABLE platform.telemetry_events;
