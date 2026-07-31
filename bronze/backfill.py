"""Bulk-backfill Bronze windows for a time range.

Airflow can backfill this pipeline natively (each run derives its window from
logical_date), but that invokes the Silver Databricks job once per window. For
large gaps it's cheaper to land the raw files here and run Silver once over all
of them.
"""



import time

from fetch_5m import HEADERS, fetch_endpoint, url_5m, write_bronze

end = int(time.time()) // 300 * 300   # now, floored to a 5-min boundary
start = end - 7200                    # 24 hours earlier

for i in range(start, end, 300):
    try:
        write_bronze(fetch_endpoint(url_5m, params={"timestamp": i}, headers=HEADERS))
    except Exception as e:   # noqa: BLE001
        print(f"Error at {i}: {e}")
    finally:
        time.sleep(1)