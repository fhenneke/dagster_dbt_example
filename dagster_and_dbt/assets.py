from datetime import datetime
import json

import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

from dagster_and_dbt.partitions import daily_partition, weekly_partition
from dagster_and_dbt.project import dbt_project


class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props) -> dg.AssetKey:
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]

        if resource_type == "source":
            return dg.AssetKey(name)

        return super().get_asset_key(dbt_resource_props)

    def get_automation_condition(self, dbt_resource_props):
        return dg.AutomationCondition.eager()


## dagster pre-processing


@dg.asset(partitions_def=daily_partition)
def daily_raw_data(context: dg.AssetExecutionContext, database: DuckDBResource):
    partition_key = context.partition_key
    time_partition = partition_key.keys_by_dimension["time"]
    static_partition = partition_key.keys_by_dimension["static"]

    time_window = context.partition_time_window
    start_time = time_window.start.isoformat()
    end_time = time_window.end.isoformat()

    context.log.info(
        f"time_partition: {time_partition}, static_partition: {static_partition}"
    )
    context.log.info(f"start_time: {start_time}, end_time: {end_time}")

    query = f"""
        create table if not exists daily_raw_data (
            partition_date varchar, static_partition varchar, start_time varchar, end_time varchar, value integer
        );

        delete from daily_raw_data where partition_date = '{time_partition}' and static_partition = '{static_partition}';
    
        insert into daily_raw_data
        select
            '{time_partition}', '{static_partition}', '{start_time}', '{end_time}', {datetime.fromisoformat(time_partition).timestamp()};
    """

    context.log.info(f"query:\n{query}")
    with database.get_connection() as conn:
        conn.execute(query)


@dg.asset(partitions_def=weekly_partition)
def weekly_raw_data(context: dg.AssetExecutionContext, database: DuckDBResource):
    partition_key = context.partition_key
    time_partition = partition_key.keys_by_dimension["time"]
    static_partition = partition_key.keys_by_dimension["static"]

    time_window = context.partition_time_window
    start_time = time_window.start.isoformat()
    end_time = time_window.end.isoformat()

    query = f"""
        create table if not exists weekly_raw_data (
            partition_date varchar, static_partition varchar, start_time varchar, end_time varchar, value integer
        );

        delete from weekly_raw_data where partition_date = '{time_partition}' and static_partition = '{static_partition}';
    
        insert into weekly_raw_data
        select
            '{time_partition}', '{static_partition}', '{start_time}', '{end_time}', {datetime.fromisoformat(time_partition).timestamp()};
    """

    context.log.info(f"query:\n{query}")
    with database.get_connection() as conn:
        conn.execute(query)


## dbt
@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="stg_daily_raw_data mart_daily_data",
    partitions_def=daily_partition,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
)
def dbt_daily_models(
    context: dg.AssetExecutionContext,
    dbt: DbtCliResource,
):
    time_window = context.partition_time_window
    dbt_vars = {
        "start_time": time_window.start.isoformat(),
        "end_time": time_window.end.isoformat(),
    }
    yield from dbt.cli(
        ["build", "--vars", json.dumps(dbt_vars)], context=context
    ).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="stg_weekly_raw_data mart_weekly_data",
    partitions_def=weekly_partition,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
)
def dbt_weekly_models(
    context: dg.AssetExecutionContext,
    dbt: DbtCliResource,
):
    time_window = context.partition_time_window
    dbt_vars = {
        "start_time": time_window.start.isoformat(),
        "end_time": time_window.end.isoformat(),
    }
    yield from dbt.cli(
        ["build", "--vars", json.dumps(dbt_vars)], context=context
    ).stream()


## dagster post-processing
@dg.asset(
    deps=[dg.AssetKey("mart_daily_data")],
    partitions_def=daily_partition,
)
def post_processing_daily(): ...


@dg.asset(
    deps=[dg.AssetKey("mart_weekly_data")],
    partitions_def=weekly_partition,
    automation_condition=dg.AutomationCondition.eager(),
)
def post_processing_weekly(): ...
