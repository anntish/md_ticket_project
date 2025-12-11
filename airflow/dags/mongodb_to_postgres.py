import os
from datetime import datetime, timedelta, timezone
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


load_dotenv()

# ---------- SQLALCHEMY BASE ----------

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


def get_data_from_mongo(
    updated_at: datetime | None = None,
) -> Generator[dict[str, Any], None, None]:
    """
    Получение данных из MongoDB (flight_prices_calendar).

    Если updated_at не передан — берём все документы.
    Если передан — берём документы, у которых updated_at > переданного значения.

    В Mongo updated_at хранится как строка вида
    "2025-12-10T16:17:57.252607+00:00", поэтому сравниваем строками.
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
            f"Getting data from MongoDB with updated_at watermark: {updated_at}"
        )

        if updated_at is None:
            filter_condition: dict[str, Any] = {}
        else:
            # приводим watermark к UTC и в ISO-формат, как в Mongo
            iso = updated_at.astimezone(timezone.utc).isoformat()
            filter_condition = {"updated_at": {"$gt": iso}}

        logger.info(f"Mongo filter_condition: {filter_condition}")
        count = collection.count_documents(filter_condition)
        logger.info(f"Mongo documents matched: {count}")

        data = collection.find(filter_condition)

        for item in data:
            yield dict(item)
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
    batch_started_at = Column(DateTime)  # <-- новое поле

    # SCD2-поля
    valid_from_dttm = Column(DateTime, server_default=func.now())
    # 5999-12-31 – дата-заглушка, которая означает, что запись актуальна
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


def get_last_updated_at(session: Session, shift_days: int = 1) -> datetime:
    """
    Получение времени последнего обновления updated_at (аналогично AirQuality).

    :param session: Сессия SQLAlchemy
    :param shift_days: Сдвиг в днях назад – чтобы записи, которые были
                       обновлены задним числом, тоже попали в выборку.
    """
    last_updated_at: datetime | None = session.query(
        func.max(AviasalesApiLog.updated_at)
    ).scalar()

    if last_updated_at is None:
        last_updated_at = datetime(1970, 1, 1)

    last_updated_at = last_updated_at + timedelta(days=-shift_days)
    logger.info(f"Last updated_at (with shift): {last_updated_at!r}")
    return last_updated_at


def upsert_aviasales_logs(
    session: Session,
    data: Generator[dict[str, Any], None, None],
) -> None:
    """
    Загрузка данных в PostgreSQL с SCD2-логикой по aviasales_id (Mongo _id):

    - Если запись существует и была актуальной (valid_to_dttm = 5999-12-31),
      закрываем её (valid_to_dttm = NOW()).
    - Вставляем новую версию с valid_to_dttm = 5999-12-31.
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
                error=bool(item.get("error")) if "error" in item else None,
                created_at=parse_dt(item.get("created_at")),
                updated_at=parse_dt(item.get("updated_at")),
                batch_started_at=parse_dt(
                    item.get("batch_started_at")
                ),  # <-- переложили поле
            )

            session.add(avia_log)
            session.commit()

        logger.info("Upsert completed successfully.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error occurred during upsert: {e}")
        raise


def mongodb_to_postgres() -> bool:
    """
    Перемещение данных из MongoDB в PostgreSQL (ИНКРЕМЕНТАЛЬНО по updated_at).
    """
    config = get_config()
    engine = create_engine(config["postgres"]["url"])
    SessionLocal = sessionmaker(bind=engine)
    session: Session = SessionLocal()

    declare_database_in_postgres()

    try:
        # 1. Определяем watermark по updated_at из Postgres
        last_updated_at = get_last_updated_at(session)

        # 2. Забираем только данные, обновлённые после этого момента
        data = get_data_from_mongo(updated_at=last_updated_at)

        # 3. Заливаем с SCD2
        upsert_aviasales_logs(session, data)
    except Exception as e:
        session.rollback()
        logger.error(f"Error occurred in move_data_to_postgres: {e}")
        raise
    finally:
        session.close()

    return True
