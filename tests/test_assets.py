import pytest
import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_and_dbt.assets import (
    daily_raw_data,
    weekly_raw_data,
    external_daily_data,
    external_weekly_data,
)
from dagster_and_dbt.partitions import daily_partition, weekly_partition


def test_daily_raw_data_asset(test_resources, duckdb_env_var):
    """Test that daily_raw_data asset can materialize successfully."""
    # Create a partition key for testing with proper datetime format
    partition_key = dg.MultiPartitionKey({"time": "2025-05-01 00:00:00", "static": "A"})

    # Create asset execution context
    context = dg.build_asset_context(
        partition_key=partition_key,
        resources=test_resources,
    )

    # Execute the asset - resources are already in context
    result = daily_raw_data(context)

    # Verify data was inserted by querying the database
    with test_resources["database"].get_connection() as conn:
        result = conn.execute("SELECT COUNT(*) FROM daily_raw_data").fetchone()
        assert result[0] == 1

        # Verify the data content
        row = conn.execute(
            "SELECT partition_date, static_partition FROM daily_raw_data"
        ).fetchone()
        assert row[0] == "2025-05-01 00:00:00"
        assert row[1] == "A"


def test_weekly_raw_data_asset(test_resources, duckdb_env_var):
    """Test that weekly_raw_data asset can materialize successfully."""
    # Create a partition key for testing with proper datetime format
    partition_key = dg.MultiPartitionKey({"time": "2025-05-01 00:00:00", "static": "B"})

    # Create asset execution context
    context = dg.build_asset_context(
        partition_key=partition_key,
        resources=test_resources,
    )

    # Execute the asset - resources are already in context
    result = weekly_raw_data(context)

    # Verify data was inserted
    with test_resources["database"].get_connection() as conn:
        result = conn.execute("SELECT COUNT(*) FROM weekly_raw_data").fetchone()
        assert result[0] == 1

        # Verify the data content
        row = conn.execute(
            "SELECT partition_date, static_partition FROM weekly_raw_data"
        ).fetchone()
        assert row[0] == "2025-05-01 00:00:00"
        assert row[1] == "B"


def test_database_resource_connection(test_resources):
    """Test that DuckDB resource can establish connection."""
    database = test_resources["database"]

    with database.get_connection() as conn:
        # Test basic query
        result = conn.execute("SELECT 1 as test").fetchone()
        assert result[0] == 1


def test_basic_database_operations(test_resources, duckdb_env_var):
    """Test basic database operations that assets depend on."""
    database = test_resources["database"]

    with database.get_connection() as conn:
        # Test creating a table and inserting data like the assets do
        conn.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                partition_date VARCHAR, 
                static_partition VARCHAR, 
                start_time VARCHAR, 
                end_time VARCHAR, 
                value INTEGER
            )
        """)

        conn.execute("""
            INSERT INTO test_table VALUES 
            ('2025-05-01 00:00:00', 'A', '2025-05-01T00:00:00', '2025-05-02T00:00:00', 123)
        """)

        # Verify the data
        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()
        assert result[0] == 1

        row = conn.execute("SELECT * FROM test_table").fetchone()
        assert row[0] == "2025-05-01 00:00:00"
        assert row[1] == "A"
        assert row[4] == 123


def test_asset_definition_loads():
    """Test that asset definitions can be loaded without errors."""
    # Test that assets are properly defined
    assert daily_raw_data is not None
    assert weekly_raw_data is not None
    assert external_daily_data is not None
    assert external_weekly_data is not None
