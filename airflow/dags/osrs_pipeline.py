from datetime import datetime

from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)
from airflow.sdk import dag, task


SILVER_JOB_ID = "1015231839161250"


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def osrs_pipeline():

    @task
    def fetch_5m_window():
        from bronze.fetch_5m import (
            HEADERS,
            fetch_endpoint,
            url_5m,
            write_bronze,
        )

        return write_bronze(
            fetch_endpoint(url_5m, headers=HEADERS)
        )

    run_silver = DatabricksRunNowOperator(
        task_id="run_silver",
        databricks_conn_id="databricks_default",
        job_id=SILVER_JOB_ID,
    )

    fetch_5m_window() >> run_silver


osrs_pipeline()