from datetime import datetime

import dagster as dg

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

static_partition = dg.StaticPartitionsDefinition(["A", "B"])

daily_partition = dg.MultiPartitionsDefinition(
    {
        "time": dg.TimeWindowPartitionsDefinition(
            cron_schedule="0 0 * * *",
            start=datetime.fromisoformat("2025-05-01"),
            fmt=DATETIME_FORMAT,
        ),
        "static": static_partition,
    }
)

weekly_partition = dg.MultiPartitionsDefinition(
    {
        "time": dg.TimeWindowPartitionsDefinition(
            cron_schedule="0 0 * * 2",
            start=datetime.fromisoformat("2025-05-01"),
            fmt=DATETIME_FORMAT,
        ),
        "static": static_partition,
    }
)
