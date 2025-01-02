import pytest
from dagster import (
    materialize_to_memory
)
from datetime import datetime

# Import what we need from our pipeline code
from pipelines.pipeline1 import (
    nyc_taxi_data,
    weather_station_data
)


###############################################################################
# 1) Asset Tests
###############################################################################

def test_nyc_taxi_data_asset():
    """
    This test attempts to fetch data from the real NYC endpoint unless you mock it.
    In a real scenario, you might want to patch requests.get, or use local test data.
    """
    # Pick a partition (e.g., '2024-10-01') from daily_partition
    partition_key = "2024-10-01"

    result = materialize_to_memory(
        assets=[nyc_taxi_data],
        partition_key=partition_key,
        resources={},  # if you used any resource_defs, supply them here
    )

    assert result.success
    # Optionally, you can check logs, events, or outputs
    # e.g. result.asset_materializations_for_node("nyc_taxi_data")


def test_weather_station_data_asset():
    """
    Minimal test for weather_station_data. In production, you'd mock
    out requests to NOAA. Here, we run it as is, which may fetch real data.
    """
    # This asset doesn't have daily_partition (or you may have chosen to add one).
    # So we can just run it with no partitioning:
    result = materialize_to_memory([weather_station_data])
    assert result.success


