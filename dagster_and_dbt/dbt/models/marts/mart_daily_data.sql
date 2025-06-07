select
    partition_date,
    static_partition,
    start_time,
    end_time,
    value
from {{ ref('stg_daily_raw_data') }}