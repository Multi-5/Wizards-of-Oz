\set ON_ERROR_STOP on

-- Ensure the Airflow metadata database exists
SELECT 'CREATE DATABASE airflow OWNER airflow TEMPLATE template0 ENCODING ''UTF8''' 
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec

-- Ensure the warehouse database exists (it is normally created via POSTGRES_DB but this makes it idempotent)
SELECT 'CREATE DATABASE warehouse OWNER airflow TEMPLATE template0 ENCODING ''UTF8''' 
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'warehouse'
)\gexec
