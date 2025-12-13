from fastapi import status
from typing import Any
from datetime import date, datetime, timezone, timedelta

import os
import time
import random
from itertools import permutations

import requests
from loguru import logger
from pymongo import MongoClient
from urllib3.util.retry import Retry
from utils.models import AviasalesRequestParams


class RateLimitRespectingHTTPAdapter(requests.adapters.HTTPAdapter):
    """Handle HTTP 429 with retry"""

    def send(self, request, **kwargs):
        response = super().send(request, **kwargs)

        if response.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                delay = int(retry_after)
                logger.warning(f"429: Retry-After={delay}s - sleep...")
            else:
                delay = 60
                logger.warning("429: Retry-After is missing, fallback to 60s")

            eps = 0.5
            time.sleep(delay + eps)

            response = super().send(request, **kwargs)

        return response


class AviasalesFlightCollector:
    """Aviasales calendar client"""

    _API_URL = os.getenv(
        "AVIASALES_API_URL",
        "https://api.travelpayouts.com/aviasales/v3/prices_for_dates",
    )

    def __init__(self, timeout: int = 10, min_delay: float = 0.1):
        logger.debug(f"URL: {self._API_URL}")

        self.timeout = timeout
        self.min_delay = min_delay
        self._last_request_time = 0.0

        self._session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = RateLimitRespectingHTTPAdapter(max_retries=retry_strategy)
        self._session.mount("https://", adapter)

    def _enforce_rate_limit(self) -> None:
        """Enforce simple rate limit"""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
        self._last_request_time = time.monotonic()

    def __enter__(self) -> "AviasalesFlightCollector":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._session.close()

    def collect(self, params: AviasalesRequestParams) -> dict[str, Any]:
        """Call Aviasales API"""
        self._enforce_rate_limit()

        query_params = params.to_query_dict()
        # logger.debug(f"Params: {query_params}")

        headers = {
            "X-Access-Token": params.token,
            "Accept-Encoding": "gzip, deflate",
        }
        try:
            response = self._session.get(
                self._API_URL,
                params=query_params,
                headers=headers,
                timeout=self.timeout,
            )
            response.raise_for_status()

            remaining = response.headers.get("X-Rate-Limit-Remaining")
            if remaining and int(remaining) <= 10:
                logger.warning(f"Rate limit low: {remaining}/min left")

            data: dict[str, Any] = response.json()

            if not data.get("success"):
                err = data.get("error") or "Unknown API error"
                logger.error(f"API error: {err}")
                raise RuntimeError(f"Aviasales API error: {err}")

            return data

        except Exception as e:
            logger.exception(f"Error executing request: {str(e)}")
            raise


def load_city_codes(path: str) -> list[str]:
    """Load IATA city/airport codes from file, one per line, skipping comments/empty."""
    codes: list[str] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            code = line.strip().upper()
            if not code or code.startswith("#"):
                continue
            codes.append(code)
    return codes


def iter_months_current_plus_12(start: date) -> list[str]:
    """Current month + 12 months ahead = 13 months total, as 'YYYY-MM'."""
    res: list[str] = []
    year = start.year
    month = start.month

    for _ in range(13):
        res.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            month = 1
            year += 1

    return res


def iter_days_current_plus_12(start: date) -> list[str]:
    """
    Generate all days from `start` up to and including the last day of the 12th month ahead.

    Example:
        start = date(2025, 12, 12) → end = date(2026, 12, 31)
        Returns list of strings: ['2025-12-12', '2025-12-13', ..., '2026-12-31']
    """
    year = start.year
    month = start.month
    for _ in range(13):
        month += 1
        if month > 12:
            month = 1
            year += 1
    next_month_start = date(year, month, 1)
    end_date = next_month_start - timedelta(days=1)

    days: list[str] = []
    current = start
    while current <= end_date:
        days.append(current.isoformat())  # 'YYYY-MM-DD'
        current += timedelta(days=1)

    return days


def collect_calendar_to_mongo(
    *,
    one_way: bool,
    city_codes_file: str = "city_codes.txt",
) -> dict[str, Any]:
    """
    Load Aviasales calendar data into MongoDB.

    Workflow:
      1. Load IATA city/airport codes from file (one per line, skip comments/empty).
      2. Generate all ordered origin→destination pairs (origin ≠ destination).
      3. Determine date range:
           - If `one_way=True`: use 13 months (current + 12 ahead) in 'YYYY-MM' format.
           - If `one_way=False`: use all calendar days from today up to the last day
             of the 12th month ahead (e.g., 2025-12-12 → 2026-12-31).
      4. For each (origin, destination, departure) combination:
           - Build AviasalesRequestParams
           - Call Aviasales API (`prices_for_dates`)
           - Log result or error as a document in MongoDB collection `flight_prices_calendar`
             with metadata: timestamps, request/response, status, error flag.

    Mongo document schema:
    {
        "endpoint": str,
        "request": dict,               # serialized params (token excluded)
        "response": dict | str,        # full API response or exception text
        "status_code": int | None,
        "created_at": str (ISO 8601),
        "updated_at": str (ISO 8601),
        "batch_started_at": str (ISO 8601),
        "error": bool
    }

    Returns:
        dict with summary stats:
        {
            "number_tickets": int,     # total offers across all successful responses
            "number_missed": int,      # number of failed API calls
            "collection": str,         # target collection name
            "db": str,                 # target database name
            "routes": int,             # number of origin→destination pairs processed
            "periods": int,            # number of departure periods (months or days)
        }
    """
    token = os.getenv("AVIASALES_TOKEN")
    if not token:
        raise ValueError("AVIASALES_TOKEN is not set in env")

    mongo_host = os.getenv("MONGO_HOST", "mongodb")
    mongo_port = int(os.getenv("MONGO_PORT", "27017"))
    mongo_db = os.getenv("MONGO_DB") or os.getenv("MONGO_INITDB_DATABASE")
    mongo_user = os.getenv("MONGO_USERNAME") or os.getenv("MONGO_INITDB_ROOT_USERNAME")
    mongo_password = os.getenv("MONGO_PASSWORD") or os.getenv(
        "MONGO_INITDB_ROOT_PASSWORD"
    )

    if not mongo_db:
        raise ValueError("MONGO_DB/MONGO_INITDB_DATABASE is not set in env")

    city_codes = load_city_codes(city_codes_file)
    if len(city_codes) < 2:
        raise ValueError("Need at least 2 city codes in file to build pairs")

    logger.info(f"Loaded {len(city_codes)} city codes: {city_codes}")

    city_pairs = list(permutations(city_codes, 2))
    logger.info(f"Total city pairs (ordered): {len(city_pairs)}")

    periods = iter_months_current_plus_12(date.today())

    client = MongoClient(
        host=mongo_host,
        port=mongo_port,
        username=mongo_user,
        password=mongo_password,
    )

    batch_started_at = datetime.now(timezone.utc)

    try:
        db = client[mongo_db]
        collection = db["flight_prices_calendar"]

        number_tickets: int = 0
        number_missed: int = 0

        with AviasalesFlightCollector() as collector:
            for origin, destination in city_pairs:
                logger.debug(f"Processing route {origin} -> {destination}")

                for departure in periods:
                    try:
                        created_at = datetime.now(timezone.utc)

                        req = AviasalesRequestParams(
                            token=token,
                            origin=origin,
                            destination=destination,
                            one_way=one_way,
                            limit=100,  # 1000
                            sorting="price",
                            departure_at=departure,
                        )

                        response = collector.collect(req)
                        time.sleep(random.uniform(0.1, 0.3))
                        status_code = (
                            status.HTTP_200_OK
                        )  # if no exception, we assume OK

                        updated_at = datetime.now(timezone.utc)

                        doc = {
                            "endpoint": collector._API_URL,
                            "request": req.to_query_dict(),
                            "response": response,
                            "status_code": status_code,
                            "created_at": created_at.isoformat(),
                            "updated_at": updated_at.isoformat(),
                            "batch_started_at": batch_started_at.isoformat(),
                            "error": False,
                        }

                        collection.insert_one(doc)

                        number_tickets += len(response.get("data", []))

                    except Exception as e:
                        number_missed += 1

                        error_doc = {
                            "endpoint": collector._API_URL,
                            "request": (
                                req.to_query_dict() if "req" in locals() else None
                            ),
                            "response": str(e),
                            "status_code": getattr(
                                getattr(e, "response", None), "status_code", None
                            ),
                            "created_at": datetime.now(timezone.utc).isoformat(),
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                            "batch_started_at": batch_started_at.isoformat(),
                            "error": True,
                        }

                        collection.insert_one(error_doc)

                        logger.exception(
                            f"Skipping {origin}->{destination} {departure} due to error: {str(e)}"
                        )

                logger.success(
                    f"Finished route {origin}->{destination}: "
                    f"total_offers={number_tickets}, missed={number_missed}"
                )

        return {
            "number_tickets": number_tickets,
            "number_missed": number_missed,
            "collection": "flight_prices_calendar",
            "db": mongo_db,
            "routes": len(city_pairs),
            "months": len(periods),
        }
    finally:
        client.close()
