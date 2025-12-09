import os
from collections.abc import Iterable

from airflow.decorators import task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from loguru import logger
from psycopg2.extras import Json


POSTGRES_CONN_ID = "postgres_default"
MONGO_CONN_ID = "mongo_default"
MONGO_COLLECTION = "flight_prices_calendar"
POSTGRES_TABLE = "stg.aviasales_raw_offers"
INSERT_COLUMNS = ["raw_offer"]
INSERT_VALUES_PLACEHOLDER = ", ".join(["%s"] * len(INSERT_COLUMNS))


def _iter_raw_offers(docs: Iterable[dict]) -> tuple[list[tuple[Json]], int, int]:
    rows: list[tuple[Json]] = []
    total_docs = 0
    total_offers = 0

    for doc in docs:
        total_docs += 1
        offers = doc.get("data") or []

        for offer in offers:
            total_offers += 1
            rows.append((Json(offer),))

    return rows, total_docs, total_offers


def _insert_rows(pg_hook: PostgresHook, rows: list[tuple[Json]]) -> int:
    if not rows:
        return 0

    insert_sql = f"""
        INSERT INTO {POSTGRES_TABLE} ({", ".join(INSERT_COLUMNS)})
        VALUES ({INSERT_VALUES_PLACEHOLDER});
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
                logger.info(f"Inserted batch: {len(batch)} (total: {inserted})")
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Error inserting into PostgreSQL")
        raise
    finally:
        conn.close()

    return inserted


@task(task_id="mongodb_to_postgres")
def mongodb_to_postgres(**_context):
    """Load raw offers from MongoDB to Postgres STG table"""

    db_name = os.getenv("MONGO_INITDB_DATABASE")
    if not db_name:
        raise ValueError("MONGO_INITDB_DATABASE is not set in env")

    mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
    mongo_client = mongo_hook.get_conn()

    try:
        collection = mongo_client[db_name][MONGO_COLLECTION]
        rows, total_docs, total_offers = _iter_raw_offers(collection.find({}))
    finally:
        mongo_client.close()

    logger.info(f"MongoDB: docs={total_docs}, offers={total_offers}")

    if not rows:
        logger.warning("No rows to insert into PostgreSQL.")
        return {"docs": total_docs, "offers": total_offers, "inserted": 0}

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    inserted = _insert_rows(pg_hook, rows)

    logger.success(f"Done! Inserted {inserted} rows into PostgreSQL")

    return {
        "docs": total_docs,
        "offers": total_offers,
        "inserted": inserted,
        "table": POSTGRES_TABLE,
    }
