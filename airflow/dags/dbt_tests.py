from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


DBT_PROJECT_DIR = "/opt/airflow/project/dbt"
DBT_TARGET_DIR = "/tmp/dbt_test_target"


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def dbt_tests():

    BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --target-path {DBT_TARGET_DIR}",
        cwd=DBT_PROJECT_DIR,
    )


dbt_tests()