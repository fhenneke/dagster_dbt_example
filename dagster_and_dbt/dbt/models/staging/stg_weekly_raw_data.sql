{{ config(materialized='incremental', unique_key=['partition_date', 'static_partition'], tags=['weekly']) }}

select
    partition_date,
    static_partition,
    start_time,
    end_time,
    value
from {{ source('raw_data', 'weekly_raw_data') }}

{% if is_incremental() %}
    where start_time >= '{{ var("start_time") }}' 
    and start_time < '{{ var("end_time") }}'
{% endif %}