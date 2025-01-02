from dagster import (
    asset,
    DailyPartitionsDefinition,
    define_asset_job,
    ScheduleDefinition,
    sensor,
    RunRequest,
    AssetKey,
    Definitions,
    OpExecutionContext,
)
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from io import StringIO, BytesIO
from typing import List
import requests
import pandas as pd
import psycopg2


###############################################################################
# Define the shared DailyPartitionsDefinition for 7 days in October 2024
###############################################################################
daily_partition = DailyPartitionsDefinition(
    start_date="2024-10-01",  # inclusive
    end_date="2024-10-07",    # inclusive
)

CONN_STRING = "postgresql://root:root@data-db:5432/pipeline_db"


###############################################################################
# NYC Taxi Data Asset, partitioned by the 7 days
###############################################################################
def get_latest_nyc_taxi_data():
    """
    Fetches the latest NYC Taxi data from the official NYC TLC dataset (parquet).
    Tries descending months until it finds a valid file.
    """
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    today = datetime.now().strftime("%Y-%m")
    year, month = today.split("-")
    month_int = int(month)

    try:
        while month_int > 1:
            file_name = f"yellow_tripdata_{year}-{month_int}.parquet"
            response = requests.get(f"{base_url}{file_name}", stream=True)
            if response.status_code == 200:
                df = pd.read_parquet(BytesIO(response.content))
                df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.floor('h')
                df_renamed = df.rename(columns={"VendorID": "vendor_id"})
                cols = [
                    'vendor_id',
                    'tpep_pickup_datetime',
                    'passenger_count',
                    'trip_distance',
                    'fare_amount',
                    'tip_amount',
                    'total_amount'
                ]
                return df_renamed[cols]
            elif response.status_code == 403:
                month_int -= 1
            else:
                response.raise_for_status()

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching the data: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


@asset(partitions_def=daily_partition)
def nyc_taxi_data(context: OpExecutionContext) -> None:
    """
    Pulls NYC taxi data, filters by partitioned date, then writes to PostgreSQL.
    Creates daily-hour partition sub-tables in 'nyc_taxi_data'.
    """
    df = get_latest_nyc_taxi_data()
    if df is None:
        context.log.warn("No NYC Taxi data fetched.")
        return

    partition_date = context.partition_key  # e.g., "2024-10-01"
    partition_start = pd.to_datetime(partition_date)
    partition_end = partition_start + timedelta(days=1)

    filtered_df = df[
        (df['tpep_pickup_datetime'] >= partition_start) &
        (df['tpep_pickup_datetime'] < partition_end)
    ]

    # Add partitioning columns
    filtered_df['date'] = filtered_df['tpep_pickup_datetime'].dt.date
    filtered_df['hour'] = filtered_df['tpep_pickup_datetime'].dt.hour

    engine = create_engine(CONN_STRING)

    # Create partitions dynamically
    with psycopg2.connect(CONN_STRING) as conn:
        with conn.cursor() as cur:
            for date, hour in filtered_df[['date', 'hour']].drop_duplicates().itertuples(index=False):
                partition_name = f"nyc_taxi_data_{str(date).replace('-', '_')}_{hour}"
                cur.execute(f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_name = '{partition_name}'
                        ) THEN
                            CREATE TABLE pipeline_db.public.{partition_name} 
                            PARTITION OF pipeline_db.public.nyc_taxi_data
                            FOR VALUES FROM ('{date}', {hour}) TO ('{date}', {hour + 1});
                        END IF;
                    END $$;        
                """)

    # Write data to main partitioned table
    filtered_df.to_sql(
        "nyc_taxi_data",
        engine,
        if_exists="append",
        index=False,
    )

    context.log.info(f"Saved taxi data for {partition_date} to PostgreSQL.")


###############################################################################
#  NOAA Weather Data Asset
###############################################################################
def fetch_noaa_data(station_ids: List[str], year: str) -> pd.DataFrame:
    """
    Fetches hourly weather data for a list of station IDs for a given year from NOAA.
    """
    base_url = "https://www.ncei.noaa.gov/data/global-hourly/access/"
    weather_data = []

    for station_id in station_ids:
        try:
            url = base_url + f"{year}/{station_id}.csv"
            response = requests.get(url)
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text))
                weather_data.append(df)
            else:
                print(f"Data for year:{year} station_id:{station_id} not found.")
        except:
            print(f"Failed to fetch data for year:{year}, station_id:{station_id}.")

    if not weather_data:
        print("No data found for the specified station IDs.")
        return pd.DataFrame()

    compiled_df = pd.concat(weather_data, ignore_index=True)

    required_cols = ["STATION", "DATE", "WND", "TMP", "DEW", "KA1"]
    for col in required_cols:
        if col not in compiled_df.columns:
            compiled_df[col] = None  

    compiled_df = compiled_df[required_cols]
    compiled_df['DATE'] = pd.to_datetime(compiled_df['DATE']).dt.floor('h')

    compiled_df = compiled_df.rename(columns={
        "STATION": "station_id",
        "DATE": "date",
        "WND": "wnd",
        "TMP": "tmp",
        "DEW": "dew",
        "KA1": "ka1",
    })
    return compiled_df


@asset
def weather_station_data():
    df = fetch_noaa_data(["01001099999", "01001499999", "01002099999", "01003099999", "01006099999"],
                         "2024")
    df["str_dt"] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    filtered_df = df.loc[(df['str_dt'] >= '2024-10-01') & (df['str_dt'] <= '2024-10-07')]
    filtered_df = filtered_df.drop("str_dt", axis=1)
    engine = create_engine(CONN_STRING)
    filtered_df.to_sql(
        "weather_data",
        engine,
        if_exists="append",
        index=False,
    )
    return "Data materialized to PostgreSQL database"

###############################################################################
# Combined Data Asset
###############################################################################
@asset(
    partitions_def=daily_partition,
    non_argument_deps={
        AssetKey("nyc_taxi_data"),
        AssetKey("weather_station_data"),
    },
)
def combined_data_asset(context: OpExecutionContext):
    """
    Combines NYC Taxi data and Weather data for the given partition date, then
    writes merged data to 'merged_tbl' in PostgreSQL.
    """
    partition_date = context.partition_key
    partition_dt = pd.to_datetime(partition_date)
    next_day = partition_dt + timedelta(days=1)

    engine = create_engine(CONN_STRING)

    # Fetch relevant taxi data rows from this date
    taxi_query = f"""
        SELECT *
        FROM nyc_taxi_data 
        WHERE tpep_pickup_datetime >= '{partition_dt}'
          AND tpep_pickup_datetime < '{next_day}'
    """
    taxi_data = pd.read_sql(taxi_query, engine)

    # Fetch relevant weather data rows from this date
    weather_query = f"""
        SELECT *
        FROM weather_data
    """
    weather_data = pd.read_sql(weather_query, engine)

    if taxi_data.empty or weather_data.empty:
        context.log.info(
            f"No combine operation: taxi_data({len(taxi_data)}), "
            f"weather_data({len(weather_data)})"
        )
        return "No data to merge."

    weather_data = weather_data.rename(columns={"date": "date_ts"})

    # Merge on exact hour timestamps
    merged_data = pd.merge(
        taxi_data,
        weather_data,
        how="inner",
        left_on="tpep_pickup_datetime",
        right_on="date_ts",
    ).drop(columns=["date_ts"])

    merged_data.to_sql(
        "merged_tbl",
        engine,
        if_exists="append", 
        index=False,
    )

    context.log.info(f"Merged data for {partition_date}.")
    return f"Merged data for {partition_date}."

###############################################################################
# JOBS
###############################################################################
taxi_job = define_asset_job(
    name="nyc_taxi_job",
    selection=["nyc_taxi_data"],
    partitions_def=daily_partition,
)

weather_job = define_asset_job(
    name="weather_job",
    selection=["weather_station_data"]
)

combined_job = define_asset_job(
    name="combined_job",
    selection=["combined_data_asset"],
    partitions_def=daily_partition,
)


###############################################################################
# Schedules:
###############################################################################
nyc_taxi_schedule = ScheduleDefinition(
    job=taxi_job,
    cron_schedule="0 20 * * *",  # every day at 8 PM
    name="nyc_taxi_daily_8pm_schedule",
)

weather_schedule = ScheduleDefinition(
    job=weather_job,
    cron_schedule="0 8 * * 1",  # every Monday at 8 AM
    name="weather_monday_8am_schedule",
)


###############################################################################
# Sensor to run combined_data_asset every time nyc_taxi_data finishes
###############################################################################
@sensor(job=combined_job)
def combined_after_taxi_sensor(context):
    """
    A sensor that looks for newly completed partitions of nyc_taxi_data
    and triggers the combined_data_asset for that same partition.
    """
    asset_key = AssetKey("nyc_taxi_data")
    last_cursor = context.cursor or "0"
    events = list(
        context.instance.events_for_asset_key(
            asset_key=asset_key,
            after_cursor=int(last_cursor),
        )
    )

    if not events:
        return

    # For each new event, trigger a run of combined_data_asset, passing along
    # the same partition_key if available
    for event_record in events:
        # event_record.partition_key tells us which partition was just materialized
        partition_key = event_record.partition_key
        if partition_key is not None:
            run_request = RunRequest(
                run_key=f"{partition_key}-{event_record.storage_id}",
                partition_key=partition_key,
            )
            yield run_request

        # Update cursor
        context.update_cursor(str(event_record.storage_id))


###############################################################################
# Definitions object
###############################################################################
defs = Definitions(
    assets=[
        nyc_taxi_data,
        weather_station_data,
        combined_data_asset,
    ],
    jobs=[
        taxi_job,
        weather_job,
        combined_job,
    ],
    schedules=[
        nyc_taxi_schedule,
        weather_schedule,
    ],
    sensors=[
        combined_after_taxi_sensor,
    ],
)