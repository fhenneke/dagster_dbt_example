import dagster as dg

from dagster_and_dbt import assets
from dagster_and_dbt.resources import database_resource, dbt_resource

all_assets = dg.load_assets_from_modules([assets])
all_resources = {"database": database_resource, "dbt": dbt_resource}

defs = dg.Definitions(
    assets=all_assets,
    resources=all_resources,
)
