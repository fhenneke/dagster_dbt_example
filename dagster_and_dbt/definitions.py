import dagster as dg

from dagster_and_dbt import assets
from dagster_and_dbt.jobs import weekly_materialization_job
from dagster_and_dbt.resources import database_resource#, dbt_resource

all_assets = dg.load_assets_from_modules([assets])
# all_jobs = [weekly_materialization_job]
all_resources = {"database": database_resource}

defs = dg.Definitions(
    assets = all_assets,
    # jobs = all_jobs,
    resources = all_resources,
)
