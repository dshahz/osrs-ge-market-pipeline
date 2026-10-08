from datetime import datetime, timedelta, timezone

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import dag

SILVER_JOB_ID = "420996649616998"


@dag(
    # Hourly, at 7 past. Every Databricks job run pays serverless startup on top
    # of the work itself, so running Silver after every 5-minute window meant up
    # to 288 job runs a day and exhausted the Free Edition quota (Databricks
    # responds with FEATURE_DISABLED and refuses new runs). Batching the hour's
    # 12 windows into one run cuts that to 24. The offset gives the last window
    # of the hour time to land before Silver reads it.
    #
    # There is no task dependency on ingestion, and none is needed: Silver reads
    # every file in the Bronze volume and MERGEs on (item_id, window_timestamp),
    # so a window that lands after this run starts is simply picked up next hour.
    schedule="7 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    # Without this Airflow allows 16 concurrent runs. If one run hangs, the
    # scheduler keeps firing new ones and each triggers another Databricks job,
    # which is how a single stuck run turned into an account-level block.
    max_active_runs=1,
    tags=["osrs", "silver", "databricks"],
)
def osrs_silver():

    DatabricksRunNowOperator(
        task_id="run_silver",
        databricks_conn_id="databricks_default",
        job_id=SILVER_JOB_ID,
        # No retries: the common failure is the quota block, and retrying into
        # it only spends more of the quota. The next hourly run catches up.
        retries=0,
        # If Airflow kills the task on timeout, the operator cancels the
        # Databricks run too, so a hung run can't keep burning compute.
        execution_timeout=timedelta(minutes=30),
    )


osrs_silver()
