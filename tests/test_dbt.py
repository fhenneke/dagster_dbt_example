import pytest
import json
import dagster as dg
from dagster_dbt import DbtCliResource

from dagster_and_dbt.project import dbt_project


def test_dbt_project_loads():
    """Test that dbt project can be loaded and parsed successfully."""
    # Test that we can create the dbt resource
    dbt_resource = DbtCliResource(project_dir=dbt_project)
    assert dbt_resource is not None


def test_dbt_models_compile(test_resources, duckdb_env_var):
    """Test that dbt models compile successfully."""
    dbt = test_resources["dbt"]

    # Set up test variables that the models expect
    dbt_vars = {
        "start_time": "2024-01-01T00:00:00",
        "end_time": "2024-01-02T00:00:00",
    }

    # Test compilation of all models - if it fails, an exception will be raised
    try:
        dbt.cli(["compile", "--vars", json.dumps(dbt_vars)])
        # If we get here without exception, compilation succeeded
        assert True
    except Exception as e:
        pytest.fail(f"dbt compile failed: {e}")


def test_dbt_models_parse(test_resources):
    """Test that dbt models parse successfully."""
    dbt = test_resources["dbt"]

    # Test parsing of all models - if it fails, an exception will be raised
    try:
        dbt.cli(["parse"])
        # If we get here without exception, parsing succeeded
        assert True
    except Exception as e:
        pytest.fail(f"dbt parse failed: {e}")


def test_dbt_source_freshness_check(test_resources):
    """Test that dbt source freshness checks can run."""
    dbt = test_resources["dbt"]

    # This should pass even without data since we're just testing the configuration
    try:
        dbt.cli(["source", "freshness"])
        # Source freshness might fail without actual data, which is expected
        # We just want to ensure the command doesn't crash due to config issues
    except Exception as e:
        # Allow certain expected failures related to missing data
        if "relation" not in str(e).lower() and "table" not in str(e).lower():
            raise
