from datetime import datetime

import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_and_dbt.partitions import daily_partition, weekly_partition

## dagster pre-processing

@dg.asset(
    partitions_def=daily_partition
)
def pre_processing_daily(context: dg.AssetExecutionContext, database: DuckDBResource):
    partition_date_str = context.partition_key
    context.log.info(f"partition_date_str: {partition_date_str}")

    query = f"""
        create table if not exists daily_raw_data (
            partition_date varchar, value integer
        );

        delete from daily_raw_data where partition_date = '{partition_date_str}';
    
        insert into daily_raw_data
        select
            '{partition_date_str}', {datetime.fromisoformat(partition_date_str).timestamp()};
    """

    context.log.info(f"query:\n{query}")
    with database.get_connection() as conn:
        conn.execute(query)

@dg.asset(
    partitions_def=weekly_partition
)
def pre_processing_weekly(context: dg.AssetExecutionContext, database: DuckDBResource):
    partition_week_str = context.partition_key

    query = f"""
        create table if not exists weekly_raw_data (
            partition_date varchar, value integer
        );

        delete from weekly_raw_data where partition_date = '{partition_week_str}';
    
        insert into weekly_raw_data
        select
            '{partition_week_str}', {datetime.fromisoformat(partition_week_str).timestamp()};
    """

    context.log.info(f"query:\n{query}")
    with database.get_connection() as conn:
        conn.execute(query)

## dbt

## dagster post-processing
@dg.asset(
    deps=[pre_processing_daily],
    partitions_def=daily_partition,
)
def post_processing_daily():
    ...

@dg.asset(
    deps=[
        # dg.AssetDep(
        #     pre_processing_daily,
        #     partition_mapping=dg.TimeWindowPartitionMapping(start_offset=-6, end_offset=0)
        # ),
        pre_processing_daily,
        pre_processing_weekly],
    partitions_def=weekly_partition,
    automation_condition=dg.AutomationCondition.eager(),
)
def post_processing_weekly():
    ...
