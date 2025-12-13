from __future__ import annotations

import os
from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator

from common import default_args


DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", DBT_PROJECT_DIR)


with DAG(
    dag_id="dbt_pipeline",
    default_args=default_args,
    description="Run dbt models (stg/ods/dm) and generate Elementary report",
    schedule_interval="@hourly",
    start_date=datetime(2025, 12, 1),
    is_paused_upon_creation=False,
    catchup=False,
    tags=["dbt", "elt", "elementary"],
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Elementary report needs dbt artifacts; tests may warn/fail on anomalies.
    # We don't block report generation on test failures.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} || true"
        ),
    )

    edr_report = BashOperator(
        task_id="edr_report",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"mkdir -p elementary_reports && "
            f"edr report --profiles-dir {DBT_PROFILES_DIR} --target-path elementary_reports || true"
        ),
    )

    dbt_deps >> dbt_run >> dbt_test >> edr_report
