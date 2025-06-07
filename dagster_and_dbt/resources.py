import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource

from dagster_and_dbt.project import dbt_project

database_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE"),
)

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)
