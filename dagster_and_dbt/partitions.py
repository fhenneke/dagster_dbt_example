from datetime import datetime

import dagster as dg

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

daily_partition = dg.TimeWindowPartitionsDefinition(
    cron_schedule="0 0 * * *",
    start=datetime.fromisoformat("2025-05-01"),
    fmt=DATETIME_FORMAT,
)

weekly_partition = dg.TimeWindowPartitionsDefinition(
    cron_schedule="0 0 * * 2",
    start=datetime.fromisoformat("2025-05-01"),
    fmt=DATETIME_FORMAT,
)
