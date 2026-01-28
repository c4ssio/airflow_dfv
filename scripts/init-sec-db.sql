-- Initialize SEC data database and schema
-- This script runs on first Postgres startup via docker-entrypoint-initdb.d

-- Note: This runs as user 'airflow' which is the POSTGRES_USER
-- The database 'sec_data' needs to be created first

-- Create sec_data database
CREATE DATABASE sec_data;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE sec_data TO airflow;
