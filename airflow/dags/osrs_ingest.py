from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

WINDOW_SECONDS = 300
# Number of windows to look back. One window is not enough: the window that
# closes at the run's own logical date has had no time to be aggregated and
# published upstream, so an on-time run gets an empty response while a late run
# gets real data. Two windows guarantees the target window closed a full 5
# minutes before the request goes out.
WINDOW_LAG = 2


@dag(
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    # Ingestion only lands a file in a Unity Catalog volume through the Files
    # API. It never starts Databricks compute, so running it every 5 minutes
    # costs nothing against the Free Edition quota. Silver runs on its own,
    # slower schedule in osrs_silver.py.
    max_active_runs=1,
    default_args={
        # The window is derived from the logical date, so a retry re-requests
        # the exact same frozen window. Retrying is safe and covers the API's
        # occasional blips and a window that hasn't been published yet.
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=2),
    },
    tags=["osrs", "bronze"],
)
def osrs_ingest():

    @task
    def fetch_5m_window(logical_date=None):
        from bronze.common import fetch_endpoint
        from bronze.fetch_5m import url_5m, write_bronze

        # Derive the target window from the interval this run represents, not from
        # wall-clock time. A retried or backfilled run therefore fetches the same
        # frozen window it was always meant to, which is what makes the load
        # idempotent end to end.
        #
        # Flooring to a multiple of WINDOW_SECONDS is required because the API only
        # accepts timestamps on a 5-minute boundary.
        window_ts = (
            int(logical_date.timestamp()) // WINDOW_SECONDS * WINDOW_SECONDS
            - WINDOW_LAG * WINDOW_SECONDS
        )

        response = fetch_endpoint(
            url_5m, params={"timestamp": window_ts}
        )
        return write_bronze(response)

    fetch_5m_window()


osrs_ingest()
