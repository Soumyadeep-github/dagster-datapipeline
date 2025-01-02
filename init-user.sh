#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 -U root -d pipeline_db -tc "SELECT 1 FROM pg_user WHERE usename = 'my_user'" | grep -q 1 || psql -v ON_ERROR_STOP=1 -U root -d pipeline_db -c "CREATE ROLE dagster LOGIN PASSWORD 'dagster';"

psql -v ON_ERROR_STOP=1 -U root -d pipeline_db <<-EOSQL
    ALTER ROLE dagster CREATEDB;
    SELECT 'CREATE DATABASE output OWNER dagster' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'output')\gexec
EOSQL
    
psql -v ON_ERROR_STOP=1 -U root -d pipeline_db <<-EOSQL
    -- Drop the existing table if it exists
    -- DROP TABLE IF EXISTS merged_tbl CASCADE;

    -- Create the partitioned table
    CREATE TABLE IF NOT EXISTS pipeline_db.public.nyc_taxi_data (
            id SERIAL NOT NULL,
            vendor_id INT,
            tpep_pickup_datetime TIMESTAMP,
            passenger_count INT,
            trip_distance FLOAT,
            fare_amount FLOAT,
            tip_amount FLOAT,
            total_amount FLOAT,
            date DATE NOT NULL,
            hour INT NOT NULL
    ) PARTITION BY RANGE (date, hour);

    CREATE TABLE IF NOT EXISTS pipeline_db.public.weather_data (
            id SERIAL NOT NULL,
            station_id VARCHAR,
            date TIMESTAMP NOT NULL,
            wnd VARCHAR,
            tmp VARCHAR,
            dew VARCHAR,
            ka1 VARCHAR
    );

    CREATE TABLE IF NOT EXISTS pipeline_db.public.merged_tbl (
            id SERIAL NOT NULL,
            vendor_id INT,
            tpep_pickup_datetime TIMESTAMP,
            passenger_count INT,
            trip_distance FLOAT,
            fare_amount FLOAT,
            tip_amount FLOAT,
            total_amount FLOAT,
            station_id VARCHAR,
            date DATE NOT NULL,
            wnd VARCHAR,
            tmp VARCHAR,
            dew VARCHAR,
            ka1 VARCHAR,
            hour INT NOT NULL
    );
EOSQL

