from datetime import datetime

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

DBT_PROJECT_DIR = "/opt/airflow/project/dbt"
SILVER_JOB_ID = "1015231839161250"
DBT_TARGET_DIR = "/tmp/dbt_target"

@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def osrs_pipeline():

    @task
    def fetch_5m_window():
        from bronze.fetch_5m import fetch_endpoint, write_bronze, url_5m, HEADERS

        return write_bronze(fetch_endpoint(url_5m, headers=HEADERS))

    run_silver = DatabricksRunNowOperator(
        task_id="run_silver",
        databricks_conn_id="databricks_default",
        job_id=SILVER_JOB_ID,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --target-path {DBT_TARGET_DIR}",
        cwd=DBT_PROJECT_DIR,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --target-path {DBT_TARGET_DIR}",
        cwd=DBT_PROJECT_DIR,
    )

    fetch_5m_window() >> run_silver >> dbt_run >> dbt_test


osrs_pipeline()