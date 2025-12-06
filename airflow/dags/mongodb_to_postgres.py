from typing import Any
import os
import json

from loguru import logger
from airflow.decorators import task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "postgres_default"
MONGO_CONN_ID = "mongo_default"
MONGO_COLLECTION = "flight_prices_calendar"

# Новая таблица
POSTGRES_TABLE = "raw.aviasales_flight_offers"


@task(task_id="mongodb_to_postgres")
def mongodb_to_postgres(**context):
    """
    RAW EL:
    - читаем документы из Mongo
    - ничего не парсим
    - складываем каждый документ в Postgres как JSONB в колонку 'response'
    - идентификатор mongo_id + created_at — отдельными полями
    """

    db_name = os.getenv("MONGO_INITDB_DATABASE")
    if not db_name:
        raise ValueError("MONGO_INITDB_DATABASE не задан!")

    # Подключаемся к Mongo
    mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
    mongo_client = mongo_hook.get_conn()
    collection = mongo_client[db_name][MONGO_COLLECTION]

    # Postgres hook
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    cursor = collection.find({})
    rows: list[tuple[Any, ...]] = []

    total_docs = 0

    for doc in cursor:
        total_docs += 1

        mongo_id = str(doc.get("_id"))
        mongo_created_at = doc.get("created_at")

        # raw JSON
        response = json.dumps(doc, default=str)

        rows.append(
            (
                mongo_id,
                response,
                mongo_created_at,
            )
        )

    logger.info(f"MongoDB: прочитано документов={total_docs}")

    if not rows:
        logger.warning("Нет строк для вставки.")
        return {"docs": total_docs, "inserted": 0, "table": POSTGRES_TABLE}

    columns = [
        "mongo_id",
        "response",  # ← переименовано
        "mongo_created_at",
    ]

    insert_sql = f"""
        INSERT INTO {POSTGRES_TABLE} (
            {", ".join(columns)}
        ) VALUES (
            {", ".join(["%s"] * len(columns))}
        )
        ON CONFLICT (mongo_id) DO NOTHING;
    """

    batch_size = 1000
    inserted = 0

    conn = pg_hook.get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                cur.executemany(insert_sql, batch)
                inserted += len(batch)
                logger.info(f"Вставлено батчем: {len(batch)} (всего: {inserted})")
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Ошибка при вставке.")
        raise
    finally:
        conn.close()

    logger.success(f"Готово! Вставлено {inserted} строк в {POSTGRES_TABLE}")

    return {
        "docs": total_docs,
        "inserted": inserted,
        "table": POSTGRES_TABLE,
    }
