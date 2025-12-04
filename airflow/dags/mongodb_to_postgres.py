# mongodb_to_postgres.py

from datetime import datetime, date
from typing import Any
import os

from loguru import logger
from airflow.decorators import task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "postgres_default"
MONGO_CONN_ID = "mongo_default"
MONGO_COLLECTION = "flight_prices_calendar"
POSTGRES_TABLE = "aviasales.aviasales_flight_offers"


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(value)
    except Exception:
        logger.warning(f"Не удалось распарсить datetime: {value!r}")
        return None


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(value).date()
    except Exception:
        logger.warning(f"Не удалось распарсить date: {value!r}")
        return None


@task(task_id="mongodb_to_postgres")
def mongodb_to_postgres(**context):
    """
    EL:
    - читаем документы из Mongo
    - разворачиваем data[]
    - вставляем в Postgres батчами (через psycopg2), с ON CONFLICT DO NOTHING
    """

    db_name = os.getenv("MONGO_INITDB_DATABASE")
    if not db_name:
        raise ValueError("MONGO_INITDB_DATABASE не задан в переменных окружения!")

    # MongoDB
    mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
    mongo_client = mongo_hook.get_conn()
    collection = mongo_client[db_name][MONGO_COLLECTION]

    # Postgres
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    cursor = collection.find({})
    rows: list[tuple[Any, ...]] = []

    total_docs = 0
    total_offers = 0

    for doc in cursor:
        total_docs += 1

        currency = doc.get("currency")
        calendar_departure_date = _parse_date(doc.get("departure_at"))
        mongo_created_at = _parse_dt(doc.get("created_at"))

        if mongo_created_at is None:
            logger.warning(f"Документ без created_at, _id={doc.get('_id')}")
            continue

        offers = doc.get("data") or []

        for offer in offers:
            total_offers += 1

            rows.append(
                (
                    offer.get("origin"),
                    offer.get("destination"),
                    offer.get("origin_airport"),
                    offer.get("destination_airport"),
                    offer.get("price"),
                    currency,
                    offer.get("airline"),
                    offer.get("flight_number"),
                    _parse_dt(offer.get("departure_at")),
                    _parse_dt(offer.get("return_at")),
                    offer.get("transfers"),
                    offer.get("return_transfers"),
                    offer.get("duration"),
                    offer.get("duration_to"),
                    offer.get("duration_back"),
                    offer.get("link"),
                    calendar_departure_date,
                    mongo_created_at,
                )
            )

    logger.info(f"MongoDB: документов={total_docs}, офферов={total_offers}")

    if not rows:
        logger.warning("Нет строк для вставки в PostgreSQL.")
        return {"docs": total_docs, "offers": total_offers, "inserted": 0}

    # поля в том порядке, как в init.sql
    columns = [
        "origin",
        "destination",
        "origin_airport",
        "destination_airport",
        "price",
        "currency",
        "airline",
        "flight_number",
        "departure_at",
        "return_at",
        "transfers",
        "return_transfers",
        "duration",
        "duration_to",
        "duration_back",
        "link",
        "calendar_departure_date",
        "mongo_created_at",
    ]

    insert_sql = f"""
        INSERT INTO {POSTGRES_TABLE} (
            {", ".join(columns)}
        ) VALUES (
            {", ".join(["%s"] * len(columns))}
        )
        ON CONFLICT DO NOTHING;
    """

    # ---- батчи через psycopg2 ----
    batch_size = 1000
    inserted = 0

    conn = pg_hook.get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                # executemany = один SQL, много параметров
                cur.executemany(insert_sql, batch)
                inserted += len(batch)
                logger.info(f"Вставлено батчем: {len(batch)} (всего: {inserted})")
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Ошибка при вставке в PostgreSQL")
        raise
    finally:
        conn.close()

    logger.success(f"Готово! Вставлено {inserted} строк в PostgreSQL")

    return {
        "docs": total_docs,
        "offers": total_offers,
        "inserted": inserted,
        "table": POSTGRES_TABLE,
    }
