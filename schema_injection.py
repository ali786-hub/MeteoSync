import os
import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv

# Load database environment variables from .env
load_dotenv()

schema_sql = """
-- Create security-isolated schemas
CREATE SCHEMA IF NOT EXISTS auth;
CREATE SCHEMA IF NOT EXISTS telemetry;

-- 1. Dimension Table in 'auth' schema (User Security)
CREATE TABLE IF NOT EXISTS auth.dim_users (
    user_key SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) DEFAULT 'viewer',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Dimension Table in 'telemetry' schema (Locations)
CREATE TABLE IF NOT EXISTS telemetry.dim_locations (
    location_key SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    latitude NUMERIC(9,6) NOT NULL,
    longitude NUMERIC(9,6) NOT NULL,
    UNIQUE(latitude, longitude)
);

-- 3. Dimension Table in 'telemetry' schema (Date/Calendar Context)
CREATE TABLE IF NOT EXISTS telemetry.dim_date (
    date_key INT PRIMARY KEY,         -- Format: YYYYMMDD (e.g., 20260614)
    full_date DATE UNIQUE NOT NULL,
    day_of_week VARCHAR(15) NOT NULL, -- 'Monday', 'Tuesday', etc.
    is_weekend BOOLEAN NOT NULL,
    month_name VARCHAR(15) NOT NULL,  -- 'January', etc.
    season VARCHAR(15) NOT NULL       -- 'Winter', 'Spring', etc.
);

-- 4. Fact Table in 'telemetry' schema (Weather Metrics)
CREATE TABLE IF NOT EXISTS telemetry.fact_climate_telemetry (
    fact_key SERIAL PRIMARY KEY,
    location_key INT NOT NULL REFERENCES telemetry.dim_locations(location_key) ON DELETE CASCADE,
    date_key INT NOT NULL REFERENCES telemetry.dim_date(date_key),
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    temperature_celsius NUMERIC(5,2) NOT NULL,
    humidity_percentage NUMERIC(5,2) NOT NULL,
    precipitation_mm NUMERIC(5,2) DEFAULT 0.0,
    cloud_cover_percentage NUMERIC(5,2) DEFAULT 0.0,
    wind_speed_kmh NUMERIC(5,2) DEFAULT 0.0,
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(location_key, recorded_at)
);

-- Indexing for Timeseries Analytics optimization
CREATE INDEX IF NOT EXISTS idx_telemetry_recorded_at ON telemetry.fact_climate_telemetry (recorded_at);
CREATE INDEX IF NOT EXISTS idx_telemetry_location_key ON telemetry.fact_climate_telemetry (location_key);
CREATE INDEX IF NOT EXISTS idx_telemetry_date_key ON telemetry.fact_climate_telemetry (date_key);

-- Seed initial tracking locations
INSERT INTO telemetry.dim_locations (city_name, latitude, longitude) 
VALUES 
    ('Islamabad - Core', 33.6844, 73.0479),
    ('Islamabad - North (E-7)', 33.7104, 73.0298),
    ('Islamabad - Industrial (I-9)', 33.6573, 73.0592),
    ('Islamabad - South (DHA Phase 2)', 33.5250, 73.0933)
ON CONFLICT (latitude, longitude) DO NOTHING;

-- Auto-Generate 6 years of calendar data for the Date Dimension using PostgreSQL functions!
INSERT INTO telemetry.dim_date (date_key, full_date, day_of_week, is_weekend, month_name, season)
SELECT 
    TO_CHAR(datum, 'YYYYMMDD')::INT AS date_key,
    datum AS full_date,
    TO_CHAR(datum, 'FMDay') AS day_of_week,
    EXTRACT(ISODOW FROM datum) IN (6, 7) AS is_weekend,
    TO_CHAR(datum, 'FMMonth') AS month_name,
    CASE 
        WHEN EXTRACT(MONTH FROM datum) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM datum) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM datum) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Autumn'
    END AS season
FROM (SELECT generate_series('2024-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS datum) d
ON CONFLICT (date_key) DO NOTHING;
"""

def inject_schema():
    conn = None
    try:
        db_name = os.getenv("DB_NAME")
        print(f"Connecting to database '{db_name}'...")
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=db_name,
            port=os.getenv("DB_PORT")
        )
        
        cursor = conn.cursor()
        print("Connection established. Injecting Isolated Schema tables and indexes...")
        
        # Execute schema definition DDL
        cursor.execute(schema_sql)
        
        # Commit database transaction
        conn.commit()
        print("SUCCESS: Relational Schemas ('auth' & 'telemetry' with dim_date) injected cleanly!")
        
    except (Exception, Error) as error:
        print("Error while injecting schema:", error)
        if conn:
            conn.rollback()
            
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Database connection closed cleanly.")

if __name__ == "__main__":
    inject_schema()