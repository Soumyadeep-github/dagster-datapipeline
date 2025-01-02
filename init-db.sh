#!/bin/bash
set -e
if [ -f /initdb.d/.env ]; then
   set -o allexport
   source /initdb.d/.env
   set +o allexport
fi
sleep 5
psql $DB_URL -v ON_ERROR_STOP=1 -tc "SELECT 1 FROM pg_user WHERE usename = '$PGUID'" | grep -q 1 || psql -v ON_ERROR_STOP=1 $DB_URL -c "CREATE ROLE $PGUID LOGIN PASSWORD '$PGUID';"
psql -v ON_ERROR_STOP=1 $DB_URL <<-EOSQL
    ALTER ROLE $PGUID CREATEDB;
    SELECT 'CREATE DATABASE output OWNER $PGUID' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'output')\gexec
EOSQL
psql -v ON_ERROR_STOP=1 $DB_URL <<-EOSQL
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
