# mongo_to_postgres.py

from datetime import datetime, timezone, date
from typing import Any
import os

from loguru import logger
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


def fetch_mongo_docs(
    mongo_conn_id: str,
    mongo_db_env_var: str = "MONGO_INITDB_DATABASE",
    collection_name: str = "flight_prices_calendar",
) -> list[dict[str, Any]]:
    """
    Забирает ВСЕ документы из указанной коллекции MongoDB.
    Использует Airflow MongoHook и имя базы из переменной окружения.
    """
    db_name = os.getenv(mongo_db_env_var)
    if not db_name:
        raise ValueError(f"{mongo_db_env_var} не задана в переменных окружения!")

    mongo_hook = MongoHook(conn_id=mongo_conn_id)
    client = mongo_hook.get_conn()
    db = client[db_name]
    collection = db[collection_name]

    docs = list(collection.find({}))
    logger.info(f"Найдено {len(docs)} документов в MongoDB.{collection_name}")
    return docs


def upsert_docs_to_postgres_raw(
    docs: list[dict[str, Any]],
    postgres_conn_id: str,
    table_name: str = "flight_prices_calendar_raw",
) -> int:
    """
    Кладёт документы из Mongo в Postgres в таблицу RAW.
    - origin, destination, departure_at → отдельные колонки
    - payload JSONB → весь документ без служебных полей
    - created_at → из Mongo (если есть)
    - loaded_at → текущее время (момент загрузки в PG)

    Вставка с UPSERT по (origin, destination, departure_at).
    """

    if not docs:
        logger.warning("Список документов пуст, нечего загружать в Postgres")
        return 0

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        origin       TEXT NOT NULL,
        destination  TEXT NOT NULL,
        departure_at DATE NOT NULL,
        payload      JSONB,
        created_at   TIMESTAMPTZ,
        loaded_at    TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (origin, destination, departure_at)
    );
    """

    insert_sql = f"""
        INSERT INTO {table_name} (
            origin, destination, departure_at, payload, created_at, loaded_at
        ) VALUES %s
        ON CONFLICT (origin, destination, departure_at) DO UPDATE SET
            payload    = EXCLUDED.payload,
            created_at = EXCLUDED.created_at,
            loaded_at  = EXCLUDED.loaded_at;
    """

    rows = []

    for d in docs:
        try:
            origin = d["origin"]
            destination = d["destination"]

            # В Mongo ты сохраняешь departure_at как "YYYY-MM-DD"
            departure_at_str = d["departure_at"]
            departure_at = date.fromisoformat(departure_at_str)

            # created_at может быть строкой в ISO формате
            created_at_raw = d.get("created_at")
            if isinstance(created_at_raw, str):
                try:
                    created_at = datetime.fromisoformat(created_at_raw)
                except Exception:
                    created_at = None
            else:
                created_at = None

            # payload: весь документ без служебных/ключевых полей
            payload = {
                k: v
                for k, v in d.items()
                if k not in ["_id", "origin", "destination", "departure_at"]
            }

            rows.append(
                (
                    origin,
                    destination,
                    departure_at,
                    payload,
                    created_at,
                    datetime.now(timezone.utc),  # loaded_at
                )
            )
        except Exception as e:
            logger.exception(f"Не удалось обработать документ {d.get('_id')}: {e}")

    if not rows:
        logger.warning("После нормализации нет строк для вставки в Postgres")
        return 0

    with conn:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            execute_values(cur, insert_sql, rows)

    logger.success(
        f"Успешно загружено {len(rows)} строк в Postgres в таблицу {table_name}"
    )
    return len(rows)


def mongodb_to_postgres_full_reload(
    mongo_conn_id: str = "mongo_default",
    postgres_conn_id: str = "postgres_default",
    mongo_db_env_var: str = "MONGO_INITDB_DATABASE",
    collection_name: str = "flight_prices_calendar",
    table_name: str = "flight_prices_calendar_raw",
) -> int:
    """
    Высокоуровневая функция:
    - забирает ВСЕ документы из Mongo
    - полностью перезаливает их в RAW-таблицу в Postgres (через upsert)
    """
    docs = fetch_mongo_docs(
        mongo_conn_id=mongo_conn_id,
        mongo_db_env_var=mongo_db_env_var,
        collection_name=collection_name,
    )
    return upsert_docs_to_postgres_raw(
        docs=docs,
        postgres_conn_id=postgres_conn_id,
        table_name=table_name,
    )


if __name__ == "__main__":
    # Опционально: возможность запустить как отдельный скрипт
    inserted = mongodb_to_postgres_full_reload()
    print(f"Inserted/updated {inserted} rows into Postgres")
