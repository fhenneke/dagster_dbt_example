{{ config(materialized='incremental', unique_key=['partition_date', 'static_partition'], tags=['weekly']) }}

with daily_aggregated as (
    select
        static_partition,
        -- Calculate Tuesday-based week start: find the Tuesday at or before each date
        -- EXTRACT(dow) returns: Sunday=0, Monday=1, Tuesday=2, Wednesday=3, etc.
        -- We want to subtract days to get to the previous Tuesday (or same day if Tuesday)
        (start_time::timestamp::date - INTERVAL '1 day' * ((EXTRACT(dow FROM start_time::timestamp) + 5) % 7))::timestamp as week_start,
        sum(value) as daily_sum
    from {{ ref('stg_daily_raw_data') }}
    {% if is_incremental() %}
    where start_time >= '{{ var("start_time") }}' 
    and start_time < '{{ var("end_time") }}'
    {% endif %}
    group by 1, 2
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