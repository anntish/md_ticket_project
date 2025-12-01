import os

import psycopg2
from loguru import logger
from pymongo import MongoClient


def check_postgres() -> bool:
    """Check PostgreSQL connection."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "md_ticket"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            connect_timeout=3,
        )
        conn.close()
        logger.info("PostgreSQL connection: OK")
        return True
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        return False


def check_mongodb() -> bool:
    """Check MongoDB connection."""
    try:
        client = MongoClient(
            host=os.getenv("MONGO_HOST", "mongodb"),
            port=int(os.getenv("MONGO_PORT", "27017")),
            username=os.getenv("MONGO_USERNAME"),
            password=os.getenv("MONGO_PASSWORD"),
            serverSelectionTimeoutMS=3000,
        )
        client.admin.command("ping")
        client.close()
        logger.info("MongoDB connection: OK")
        return True
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        return False
