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
        bash_command=(
            "set -euo pipefail; "
            'export PATH="$HOME/.local/bin:$PATH"; '
            f"cd {DBT_PROJECT_DIR}; "
            'echo "DBT_PROJECT_DIR=$(pwd)"; '
            "ls -la; "
            "dbt --version; "
            f"dbt deps --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "set -euo pipefail; "
            'export PATH="$HOME/.local/bin:$PATH"; '
            f"cd {DBT_PROJECT_DIR}; "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "set -euo pipefail; "
            'export PATH="$HOME/.local/bin:$PATH"; '
            f"cd {DBT_PROJECT_DIR}; "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} || true"
        ),
    )

    edr_report = BashOperator(
        task_id="edr_report",
        bash_command=(
            "set -euo pipefail; "
            'export PATH="$HOME/.local/bin:$PATH"; '
            f"cd {DBT_PROJECT_DIR}; "
            "mkdir -p elementary_reports; "
            f"edr report --profiles-dir {DBT_PROFILES_DIR} --target-path elementary_reports || true; "
            "if [ -f elementary_reports/elementary_report.html ]; then "
            "cp -f elementary_reports/elementary_report.html elementary_reports/index.html; "
            "fi"
        ),
    )

    dbt_deps >> dbt_run >> dbt_test >> edr_report
