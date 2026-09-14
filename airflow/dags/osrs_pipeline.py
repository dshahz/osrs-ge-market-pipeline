from datetime import datetime, timezone

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import dag, task

SILVER_JOB_ID = "420996649616998"
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
    # Without this Airflow allows 16 concurrent runs. If one run hangs, the
    # scheduler keeps firing new ones every 5 minutes and each triggers another
    # Databricks job, which is how a single stuck run turned into an account-level
    # rate limit. One run at a time; late runs queue rather than pile up.
    max_active_runs=1,
)
def osrs_pipeline():

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

    run_silver = DatabricksRunNowOperator(
        task_id="run_silver",
        databricks_conn_id="databricks_default",
        job_id=SILVER_JOB_ID,
    )

    fetch_5m_window() >> run_silver


osrs_pipeline()