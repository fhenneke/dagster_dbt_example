import pytest
import json
from unittest.mock import patch, MagicMock
import dagster as dg
from dagster import materialize

from dagster_and_dbt.assets import (
    daily_raw_data,
    weekly_raw_data,
    dbt_daily_models,
    dbt_weekly_models,
    external_daily_data,
    external_weekly_data,
)


def test_materialize_full_daily_pipeline_with_real_dbt(test_resources, duckdb_env_var):
    """Test materializing the complete daily pipeline with real dbt execution."""
    partition_key = dg.MultiPartitionKey({"time": "2025-05-01 00:00:00", "static": "A"})

    # Try to materialize the entire pipeline together with real dbt
    # Dagster should handle dependency order: raw data first, then dbt, then external export
    result = materialize(
        [daily_raw_data, dbt_daily_models, external_daily_data],
        resources=test_resources,
        partition_key=partition_key,
    )

    # If dbt fails due to missing dependencies or config issues, we'll see it here
    if not result.success:
        print(f"Pipeline failed: {result.failure_for_asset_key}")
        for asset_key, failure in result.failure_for_asset_key.items():
            print(f"Asset {asset_key} failed: {failure}")

    assert result.success, f"Pipeline failed: {result.failure_for_asset_key}"

    # Verify the complete data flow worked
    with test_resources["database"].get_connection() as conn:
        # Check raw data was created
        raw_count = conn.execute("SELECT COUNT(*) FROM daily_raw_data").fetchone()[0]
        assert raw_count == 1

        # Check staging data was created by dbt
        staging_count = conn.execute(
            "SELECT COUNT(*) FROM stg_daily_raw_data"
        ).fetchone()[0]
        assert staging_count == 1

        # Check mart data was created by dbt
        mart_count = conn.execute("SELECT COUNT(*) FROM mart_daily_data").fetchone()[0]
        assert mart_count == 1

        # Check external export was created
        export_count = conn.execute(
            "SELECT COUNT(*) FROM external_daily_data"
        ).fetchone()[0]
        assert export_count == 1

        # Verify data flows through the pipeline unchanged (dbt models just copy data)
        raw_value = conn.execute("SELECT value FROM daily_raw_data").fetchone()[0]
        mart_value = conn.execute("SELECT value FROM mart_daily_data").fetchone()[0]
        export_value = conn.execute(
            "SELECT total_value FROM external_daily_data"
        ).fetchone()[0]

        # Data should flow through unchanged: raw -> staging -> mart -> export
        assert mart_value == raw_value  # dbt models just copy the data
        assert (
            export_value == mart_value
        )  # external export aggregates (but single row = same value)


def test_materialize_full_weekly_pipeline_with_real_dbt(test_resources, duckdb_env_var):
    """Test materializing the complete weekly pipeline with real dbt execution."""
    # Weekly partitions start on Tuesday (cron: 0 0 * * 2), so use May 6th 2025 (first Tuesday)
    weekly_partition_key = dg.MultiPartitionKey(
        {"time": "2025-05-06 00:00:00", "static": "B"}
    )

    # The weekly mart model depends on daily staging data, so we need to create some daily data first
    # Create daily data for the same week (multiple days within the weekly partition)
    daily_partition_keys = [
        dg.MultiPartitionKey({"time": "2025-05-06 00:00:00", "static": "B"}),  # Tuesday
        dg.MultiPartitionKey(
            {"time": "2025-05-07 00:00:00", "static": "B"}
        ),  # Wednesday
        dg.MultiPartitionKey(
            {"time": "2025-05-08 00:00:00", "static": "B"}
        ),  # Thursday
    ]

    # First materialize daily raw data and staging for the week
    for daily_key in daily_partition_keys:
        daily_result = materialize(
            [daily_raw_data, dbt_daily_models],
            resources=test_resources,
            partition_key=daily_key,
        )
        assert daily_result.success, (
            f"Daily pipeline failed for {daily_key}: {daily_result.failure_for_asset_key}"
        )

    # Now materialize the weekly pipeline
    result = materialize(
        [weekly_raw_data, dbt_weekly_models, external_weekly_data],
        resources=test_resources,
        partition_key=weekly_partition_key,
    )

    # If dbt fails, we'll see it here
    if not result.success:
        print(f"Weekly pipeline failed: {result.failure_for_asset_key}")
        for asset_key, failure in result.failure_for_asset_key.items():
            print(f"Asset {asset_key} failed: {failure}")

    assert result.success, f"Weekly pipeline failed: {result.failure_for_asset_key}"

    # Verify the complete data flow worked
    with test_resources["database"].get_connection() as conn:
        # Check raw data was created
        raw_count = conn.execute("SELECT COUNT(*) FROM weekly_raw_data").fetchone()[0]
        assert raw_count == 1

        # Check staging data was created by dbt
        staging_count = conn.execute(
            "SELECT COUNT(*) FROM stg_weekly_raw_data"
        ).fetchone()[0]
        assert staging_count == 1

        # Check mart data was created by dbt
        mart_count = conn.execute("SELECT COUNT(*) FROM mart_weekly_data").fetchone()[0]
        assert mart_count == 1

        # Check external export was created
        export_count = conn.execute(
            "SELECT COUNT(*) FROM external_weekly_data"
        ).fetchone()[0]
        assert export_count == 1

        # Verify the complete data flow worked with Tuesday-based aggregation
        raw_value = conn.execute("SELECT value FROM weekly_raw_data").fetchone()[0]
        mart_row = conn.execute(
            "SELECT weekly_value, COALESCE(daily_sum, 0) as daily_sum FROM mart_weekly_data"
        ).fetchone()
        weekly_value = mart_row[0]
        daily_sum = mart_row[1]
        export_value = conn.execute(
            "SELECT total_value FROM external_weekly_data"
        ).fetchone()[0]

        # Calculate expected daily sum from the three daily records created
        # We created daily data for 2025-05-06, 2025-05-07, 2025-05-08 (all partition B)
        # Values are UTC timestamps: 1746489600, 1746576000, 1746662400
        expected_daily_sum = 1746489600 + 1746576000 + 1746662400  # = 5239728000
        expected_export = (
            weekly_value + daily_sum
        )  # = 1746489600 + 5239728000 = 6986217600

        # Verify the data flow
        assert weekly_value == raw_value, (
            f"Weekly value {weekly_value} should equal raw value {raw_value}"
        )
        assert daily_sum == expected_daily_sum, (
            f"Daily sum {daily_sum} should equal expected {expected_daily_sum}"
        )
        assert export_value == expected_export, (
            f"Export {export_value} should equal {weekly_value} + {daily_sum} = {expected_export}"
        )
