{{ config(materialized='incremental', unique_key=['partition_date', 'static_partition'], tags=['daily']) }}

select
    partition_date,
    static_partition,
    start_time,
    end_time,
    value
from {{ source('raw_data', 'daily_raw_data') }}

{% if is_incremental() %}
    where start_time >= '{{ var("start_time") }}' 
    and start_time < '{{ var("end_time") }}'
{% endif %}