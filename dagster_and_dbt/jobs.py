import dagster as dg

from dagster_and_dbt.assets import post_processing_weekly

weekly_materialization_job = dg.define_asset_job(
    "weekly_materialization_job",
    selection=dg.AssetSelection.assets(post_processing_weekly).upstream(),
)
