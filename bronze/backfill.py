"""Bulk-backfill Bronze windows for a time range.

Airflow can backfill this pipeline natively (each run derives its window from
logical_date), but that invokes the Silver Databricks job once per window. For
large gaps it's cheaper to land the raw files here and run Silver once over all
of them.

Run from the repo root as a module so the `bronze` package resolves:

    python -m bronze.backfill
"""

import time

from bronze.common import fetch_endpoint
from bronze.fetch_5m import url_5m, write_bronze

WINDOW_SECONDS = 300
HOURS_BACK = 24

end = int(time.time()) // WINDOW_SECONDS * WINDOW_SECONDS  # now, floored to a 5-min boundary
start = end - HOURS_BACK * 3600

for window_ts in range(start, end, WINDOW_SECONDS):
    try:
        write_bronze(fetch_endpoint(url_5m, params={"timestamp": window_ts}))
    except Exception as e:  # noqa: BLE001
        print(f"Error at {window_ts}: {e}")
    finally:
        time.sleep(1)