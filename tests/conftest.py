import os
import tempfile
from unittest.mock import patch
import pytest
import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource

from dagster_and_dbt.project import dbt_project


@pytest.fixture
def temp_database():
    """Create a temporary DuckDB database for testing."""
    # Create a proper temporary file path but don't create the file
    # DuckDB will create it when first connected
    db_path = tempfile.mktemp(suffix=".duckdb")

    yield db_path

    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def test_resources(temp_database):
    """Create test resources with temporary database."""
    # Create a test-specific dbt resource that points to the test database
    import tempfile
    import os

    # Create temporary profiles dir for this test
    with tempfile.TemporaryDirectory() as profiles_dir:
        profiles_yml_content = f"""
my_dbt_project:
  target: test
  outputs:
    test:
      type: duckdb
      path: '{temp_database}'
"""
        profiles_yml_path = os.path.join(profiles_dir, "profiles.yml")
        with open(profiles_yml_path, "w") as f:
            f.write(profiles_yml_content)

        test_dbt_resource = DbtCliResource(
            project_dir=dbt_project, profiles_dir=profiles_dir, target="test"
        )

        yield {
            "database": DuckDBResource(database=temp_database),
            "dbt": test_dbt_resource,
        }


@pytest.fixture
def duckdb_env_var(temp_database):
    """Mock the DUCKDB_DATABASE environment variable."""
    with patch.dict(os.environ, {"DUCKDB_DATABASE": temp_database}):
        yield temp_database
