from datetime import datetime

import requests
from airflow.decorators import task
from airflow.models import DAG
from loguru import logger

from common import default_args, AVIASALES_SERVICE_URL


def get_collect_data(params: dict, url: str):
    response = requests.post(url, params=params, timeout=2000)
    logger.debug(
        f"Request with one_way={params.get('one_way')} completed, status={response.status_code}"
    )

    response.raise_for_status()
    return response


@task(task_id="call_aviasales_service")
def call_aviasales_service(**context):
    """
    Call Aviasales collection service
    """
    url = f"{AVIASALES_SERVICE_URL.rstrip('/')}/api/aviasales/collect"

    one_way: bool = True

    info = dict()

    logger.info(f"Calling Aviasales collection service at {url}")
    try:
        response = get_collect_data(params={"one_way": one_way}, url=url)
        logger.debug(f"{one_way} is completed)")
        info[f"one_way(={one_way})"] = response.json()

        one_way = False
        response = get_collect_data(params={"one_way": one_way}, url=url)
        logger.debug(f"{one_way} is completed)")
        info[f"one_way(={one_way})"] = response.json()

    except Exception as exc:
        logger.exception(f"HTTP error calling service: {exc}")
        raise

    logger.info(f"Aviasales service response: {info}")
    return info


with DAG(
    dag_id="aviasales_calendar_service_loader",
    default_args=default_args,
    description="Generate load on the service: call Aviasales collection service in MongoDB",
    schedule_interval="*/50 * * * *",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["aviasales", "service", "mongodb"],
) as dag:
    call_aviasales_service()
