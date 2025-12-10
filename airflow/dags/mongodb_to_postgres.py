import os
from datetime import datetime
from typing import Any
from collections.abc import Generator

from dotenv import load_dotenv
from loguru import logger
from pymongo import MongoClient
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Boolean,
    create_engine,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.sql import func

from airflow import DAG  # type: ignore[attr-defined]
from airflow.decorators import task  # type: ignore[attr-defined]

load_dotenv()

# ---------- SQLALCHEMY BASE ----------

# Для SQLAlchemy 1.x используем declarative_base.
# mypy не любит его как тип для наследования — заглушим на уровне класса.
Base = declarative_base()


# ---------- CONFIG ----------


def get_config() -> dict[str, dict[str, Any]]:
    """
    Получение конфигурации из окружения
    """

    pg_db = os.getenv("POSTGRES_DB")
    pg_user = os.getenv("POSTGRES_USER")
    pg_password = os.getenv("POSTGRES_PASSWORD")
    pg_host = os.getenv("POSTGRES_HOST")
    pg_port = os.getenv("POSTGRES_PORT")

    # PostgreSQL URL
    pg_url = (
        "postgresql://" f"{pg_user}:{pg_password}" f"@{pg_host}:{pg_port}" f"/{pg_db}"
    )

    mongo_port_str = os.getenv("MONGO_PORT")
    if mongo_port_str is None:
        raise ValueError("MONGO_PORT is not set in environment")

    return {
        "mongo": {
            "username": os.getenv("MONGO_INITDB_ROOT_USERNAME"),
            "password": os.getenv("MONGO_INITDB_ROOT_PASSWORD"),
            "host": os.getenv("MONGO_HOST"),
            "port": int(mongo_port_str),
            "authSource": "admin",
        },
        "postgres": {
            "dbname": pg_db,
            "user": pg_user,
            "password": pg_password,
            "host": pg_host,
            "port": pg_port,
            "url": pg_url,
        },
    }


# ---------- UTILS ----------


def parse_dt(value: Any) -> datetime | None:
    """
    Преобразование ISO-строки в datetime (или возврат None).
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except Exception:
            logger.warning(f"Cannot parse datetime from value: {value!r}")
            return None
    return None


# ---------- MONGO ----------


def get_data_from_mongo() -> Generator[dict[str, Any], None, None]:
    """
    Получение ВСЕХ данных из MongoDB (flight_prices_calendar).
    """

    mongo_config = get_config()["mongo"]
    client = MongoClient(**mongo_config)

    try:
        db_name = os.getenv("MONGO_DB") or os.getenv("MONGO_INITDB_DATABASE")
        if not db_name:
            raise ValueError("MONGO_DB или MONGO_INITDB_DATABASE не заданы в окружении")

        db = client[db_name]
        collection = db["flight_prices_calendar"]

        logger.info(
            f"Getting ALL data from MongoDB: db={db_name}, "
            f"collection=flight_prices_calendar"
        )

        cursor = collection.find({})

        for doc in cursor:
            # dict(...) чтобы mypy был доволен, что это словарь
            yield dict(doc)

    finally:
        client.close()


# ---------- ORM MODEL ----------


class AviasalesApiLog(Base):  # type: ignore[misc, valid-type]
    __tablename__ = "aviasales_api_log"
    __table_args__ = {"schema": "raw"}

    id = Column(Integer, primary_key=True, autoincrement=True)

    # _id из Mongo
    aviasales_id = Column(String, index=True, nullable=False)

    # поля из Mongo-документа
    endpoint = Column(String)
    request = Column(JSONB)
    response = Column(JSONB)
    status_code = Column(Integer)
    error = Column(Boolean)

    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    batch_started_at = Column(DateTime)

    # SCD2-поля
    valid_from_dttm = Column(DateTime, server_default=func.now())
    valid_to_dttm = Column(
        DateTime,
        nullable=False,
        default=datetime(5999, 12, 31),
    )


# ---------- POSTGRES ----------


def declare_database_in_postgres() -> None:
    """
    Создание схемы и таблицы в PostgreSQL, если их ещё нет.
    """
    try:
        postgres_config = get_config()["postgres"]
        engine = create_engine(postgres_config["url"])

        with engine.connect() as connection:
            connection.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            Base.metadata.create_all(engine, checkfirst=True)
            logger.info("Schema 'raw' and table 'aviasales_api_log' ensured.")
    except Exception as e:
        logger.error(f"Error occurred while declaring DB schema: {e}")
        raise


def upsert_aviasales_logs(
    session: Session,
    data: Generator[dict[str, Any], None, None],
) -> None:
    """
    Загрузка данных в PostgreSQL с SCD2-логикой по aviasales_id (Mongo _id).
    """
    logger.info("Upserting data to PostgreSQL (raw.aviasales_api_log)")
    try:
        for item in data:
            mongo_id = str(item.get("_id"))
            logger.info(f"Upserting item with _id: {mongo_id}")

            # Закрываем предыдущую "актуальную" запись
            session.query(AviasalesApiLog).filter(
                AviasalesApiLog.aviasales_id == mongo_id,
                AviasalesApiLog.valid_to_dttm == datetime(5999, 12, 31),
            ).update({"valid_to_dttm": func.now()})

            avia_log = AviasalesApiLog(
                aviasales_id=mongo_id,
                endpoint=item.get("endpoint"),
                request=item.get("request"),
                response=item.get("response"),
                status_code=item.get("status_code"),
                error=item.get("error"),
                created_at=parse_dt(item.get("created_at")),
                updated_at=parse_dt(item.get("updated_at")),
                batch_started_at=parse_dt(item.get("batch_started_at")),
            )

            session.add(avia_log)
            session.commit()

        logger.info("Upsert completed successfully.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error occurred during upsert: {e}")
        raise


def move_data_to_postgres() -> bool:
    """
    Перемещение ВСЕХ данных из MongoDB в PostgreSQL.
    """
    config = get_config()
    engine = create_engine(config["postgres"]["url"])
    SessionLocal = sessionmaker(bind=engine)
    session: Session = SessionLocal()

    # гарантируем, что схема и таблица созданы
    declare_database_in_postgres()

    try:
        data = get_data_from_mongo()
        upsert_aviasales_logs(session, data)
    except Exception as e:
        session.rollback()
        logger.error(f"Error occurred in move_data_to_postgres: {e}")
        raise
    finally:
        session.close()

    return True


# ---------- AIRFLOW DAG ----------

with DAG(
    dag_id="el_mongo_to_postgres_aviasales",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # триггерим руками
    catchup=False,
    tags=["aviasales", "mongo", "postgres"],
) as dag:

    @task(task_id="move_data_to_postgres")
    def move_data_to_postgres_task() -> bool:
        return move_data_to_postgres()

    move_data_to_postgres_task()
