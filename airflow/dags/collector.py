from datetime import datetime

import requests
from airflow.decorators import task
from airflow.models import DAG
from loguru import logger

from common import default_args, AVIASALES_SERVICE_URL


@task(task_id="call_aviasales_service")
def call_aviasales_service(**context):
    """
    Call Aviasales collection service
    """
    url = f"{AVIASALES_SERVICE_URL.rstrip('/')}/api/aviasales/collect"

    logger.info(f"Calling Aviasales collection service at {url}")
    try:
        response = requests.post(url, timeout=300)
    except Exception as exc:
        logger.exception(f"HTTP error calling service: {exc}")
        raise

    if response.status_code != 200:
        logger.error(
            f"Service returned error: status={response.status_code}, body={response.text}"
        )
        raise RuntimeError(
            f"Failed to call Aviasales collection service, status={response.status_code}"
        )

    data = response.json()
    logger.info(f"Aviasales service response: {data}")
    return data


with DAG(
    dag_id="aviasales_calendar_service_loader",
    default_args=default_args,
    description="Generate load on the service: call Aviasales collection service in MongoDB",
    schedule_interval="@hourly",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["aviasales", "service", "mongodb"],
) as dag:
    call_aviasales_service()
