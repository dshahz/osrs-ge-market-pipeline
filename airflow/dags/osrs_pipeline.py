from datetime import datetime, timezone

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import dag, task

SILVER_JOB_ID = "1015231839161250"
WINDOW_SECONDS = 300


@dag(
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
)
def osrs_pipeline():

    @task
    def fetch_5m_window(logical_date=None):
        from bronze.fetch_5m import HEADERS, fetch_endpoint, url_5m, write_bronze

        # Derive the target window from the interval this run represents, not from
        # wall-clock time. A retried or backfilled run therefore fetches the same
        # frozen window it was always meant to, which is what makes the load
        # idempotent end to end.
        window_ts = int(logical_date.timestamp()) // WINDOW_SECONDS * WINDOW_SECONDS - WINDOW_SECONDS

        response = fetch_endpoint(
            url_5m, headers=HEADERS, params={"timestamp": window_ts}
        )
        return write_bronze(response)

    run_silver = DatabricksRunNowOperator(
        task_id="run_silver",
        databricks_conn_id="databricks_default",
        job_id=SILVER_JOB_ID,
    )

    fetch_5m_window() >> run_silver


osrs_pipeline()