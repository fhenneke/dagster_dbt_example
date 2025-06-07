{{ config(materialized='incremental', unique_key=['partition_date', 'static_partition'], tags=['weekly']) }}

with daily_aggregated as (
    select
        static_partition,
        date_trunc('week', start_time::timestamp) as week_start,
        sum(value) as daily_sum
    from {{ ref('stg_daily_raw_data') }}
    {% if is_incremental() %}
    where start_time >= '{{ var("start_time") }}' 
    and start_time < '{{ var("end_time") }}'
    {% endif %}
    group by static_partition, date_trunc('week', start_time::timestamp)
),

weekly_data as (
    select
        partition_date,
        static_partition,
        start_time,
        end_time,
        value as weekly_value
    from {{ ref('stg_weekly_raw_data') }}
    {% if is_incremental() %}
    where start_time >= '{{ var("start_time") }}' 
    and start_time < '{{ var("end_time") }}'
    {% endif %}
)

select
    w.partition_date,
    w.static_partition,
    w.start_time,
    w.end_time,
    w.weekly_value,
    d.daily_sum
from weekly_data w
left join daily_aggregated d
    on w.static_partition = d.static_partition
    and w.start_time::timestamp = d.week_start