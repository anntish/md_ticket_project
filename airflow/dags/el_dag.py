from datetime import datetime

from airflow.models import DAG

from common import default_args
from mongodb_to_postgres import mongodb_to_postgres


with DAG(
    dag_id="mongodb_to_postgres_el",
    default_args=default_args,
    description="EL: MongoDB (flight_prices_calendar) -> Postgres (aviasales.aviasales_flight_offers)",
    schedule_interval="0 */12 * * *",  # ✅ каждые 12 часов
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["aviasales", "el", "mongodb", "postgres"],
) as dag:
    mongodb_to_postgres()
