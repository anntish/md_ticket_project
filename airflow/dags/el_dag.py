from airflow.models import DAG
from airflow.decorators import task
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, date, timezone, timedelta
from loguru import logger
import sys
from collector import AviasalesFlightCollector, AviasalesRequestParams
from mongodb_to_postgres import mongodb_to_postgres_full_reload
from dotenv import load_dotenv
import os

load_dotenv()


@task(task_id="task_collect_and_load_to_mongodb")
def collect_and_load_to_mongodb(**context):
    token = os.getenv("AVIASALES_TOKEN")
    if not token:
        raise ValueError("AVIASALES_TOKEN не задан в переменных окружения!")

    number_days = 30

    mongo_hook = MongoHook(conn_id="mongo_default")
    db_name = os.getenv("MONGO_INITDB_DATABASE")
    if not db_name:
        raise ValueError("MONGO_INITDB_DATABASE не задан в переменных окружения!")

    client = mongo_hook.get_conn()
    db = client[db_name]
    collection = db["flight_prices_calendar"]

    try:
        req = AviasalesRequestParams(
            token=token,
            origin="MOW",
            destination="NYC",
            one_way=False,
            limit=1000,  # it's max limit
            sorting="price",
        )
    except Exception as e:
        logger.error(f"Неверные параметры: {e}")
        raise

    try:
        with AviasalesFlightCollector() as collector:

            number_tickets: int = 0
            number_missed: int = 0
            today = date.today()
            data_range = [today + timedelta(days=x) for x in range(number_days)]

            for idx, dep_date in enumerate(data_range):
                try:
                    req.departure_at = dep_date
                    response = collector.collect(req)
                    number_tickets += len(response.get("data", []))

                    filter_doc = {
                        "origin": req.origin,
                        "destination": req.destination,
                        "departure_at": dep_date.isoformat(),
                    }

                    update_doc = {
                        "$set": {
                            **response,
                            "created_at": datetime.now(timezone.utc).isoformat(),
                        }
                    }

                    collection.update_one(filter_doc, update_doc, upsert=True)

                except Exception as e:
                    number_missed += 1
                    logger.exception(f"Пропускаем {dep_date} из-за ошибки {str(e)}")

                logger.success(f"Response {idx} : Collected {number_tickets} offers.")

    except Exception as e:
        logger.exception(f"Critical error in data collection pipeline {str(e)}")
        sys.exit(1)

    return {
        "number_tickets": number_tickets,
        "number_missed": number_missed,
        "collection": "flight_prices_calendar",
        "db": db_name,
    }


default_args = {
    "owner": "Nikolay Teplyakov",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="aviasales_calendar_collector",
    default_args=default_args,
    description="Сбор цен по календарю из Aviasales -> MongoDB (bulk-safe) -> Postgres (full reload)",
    schedule_interval="@hourly",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["aviasales", "el", "mongodb"],
) as dag:

    collect_mongo = collect_and_load_to_mongodb()
    mongo_to_postgres = mongodb_to_postgres_full_reload()

    collect_mongo >> mongo_to_postgres
