from typing import Any, Literal
from datetime import date, datetime, timezone

import os
import time
from itertools import permutations

import requests
from loguru import logger
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import MongoClient
from urllib3.util.retry import Retry


class AviasalesRequestParams(BaseModel):
    token: str = Field(..., description="Partner API token")
    origin: str = Field(
        ...,
        min_length=2,
        max_length=3,
        pattern=r"^[A-Z]{2,3}$",
        description="Origin IATA code",
    )
    destination: str = Field(
        ...,
        min_length=2,
        max_length=3,
        pattern=r"^[A-Z]{2,3}$",
        description="Destination IATA code",
    )
    currency: Literal["RUB", "USD", "EUR"] = "RUB"
    # YYYY-MM or YYYY-MM-DD or python date
    departure_at: date | str | None = Field(
        None,
        description="Departure date (YYYY-MM or YYYY-MM-DD)",
    )
    return_at: date | str | None = Field(
        None,
        description="Return date (YYYY-MM or YYYY-MM-DD)",
    )
    one_way: bool = Field(True, description="One-way ticket flag")
    direct: bool = Field(False, description="Direct flights only flag")
    market: str | None = Field(
        None,
        pattern=r"^[a-z]{2}$",
        description="Market/country code",
    )
    limit: int = Field(
        30,
        ge=1,
        le=1000,
        description="Number of results to return",
    )
    page: int = Field(1, ge=1, description="Page number")
    sorting: Literal["price", "route"] = "price"
    unique: bool = Field(
        False,
        description="Return unique routes only",
    )

    @field_validator("origin", "destination", mode="before")
    @classmethod
    def _upper_iata(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise ValueError("IATA code must be a string")
        return v.upper()

    @field_validator("departure_at", "return_at", mode="before")
    @classmethod
    def _normalize_date_or_month(cls, v: Any) -> Any:
        """Allow date or 'YYYY-MM' / 'YYYY-MM-DD' strings."""
        if v is None:
            return v
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            v = v.strip()
            # very light validation; API will do strict check
            if len(v) in (7, 10):  # 'YYYY-MM' or 'YYYY-MM-DD'
                return v
            raise ValueError(
                "departure_at/return_at must be date or string 'YYYY-MM'/'YYYY-MM-DD'"
            )
        raise ValueError("departure_at/return_at must be date or string")

    @model_validator(mode="after")
    def _validate_one_way_requires_no_return_at(self) -> "AviasalesRequestParams":
        if self.one_way and self.return_at is not None:
            raise ValueError("If one_way=True, return_at must be None")
        return self

    def to_query_dict(self) -> dict:
        """Build query params dict"""
        d = self.model_dump(exclude_none=True, exclude={"token"})

        d["one_way"] = str(self.one_way).lower()
        d["direct"] = str(self.direct).lower()
        d["unique"] = str(self.unique).lower()

        if self.departure_at is not None:
            if isinstance(self.departure_at, date):
                d["departure_at"] = self.departure_at.isoformat()
            else:
                d["departure_at"] = self.departure_at  # string as is

        if self.return_at is not None:
            if isinstance(self.return_at, date):
                d["return_at"] = self.return_at.isoformat()
            else:
                d["return_at"] = self.return_at  # string as is

        return d


class RateLimitRespectingHTTPAdapter(requests.adapters.HTTPAdapter):
    """Handle HTTP 429 with retry"""

    def send(self, request, **kwargs):
        response = super().send(request, **kwargs)

        if response.status_code == 429:
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
        logger.debug(f"Params: {query_params}")

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


def collect_calendar_to_mongo(
    *,
    city_codes_file: str = "city_codes.txt",
) -> dict[str, Any]:
    """
    Load Aviasales calendar data into MongoDB.

    - Takes city/airport codes from a file (one per line)
    - Builds all ordered pairs (origin != destination)
    - For each pair and for each of 13 months (current + 12) requests prices_for_dates
    - Logs each API call to Mongo via collection.insert_one({
        "endpoint": ...,
        "request": ...,
        "response": ...,
        "status_code": ...,
        "created_at": ...,
        "updated_at": ...,
        "batch_started_at": ...,
        "error": ...
      })
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

    # load city codes
    city_codes = load_city_codes(city_codes_file)
    if len(city_codes) < 2:
        raise ValueError("Need at least 2 city codes in file to build pairs")

    logger.info(f"Loaded {len(city_codes)} city codes: {city_codes}")

    city_pairs = list(permutations(city_codes, 2))
    logger.info(f"Total city pairs (ordered): {len(city_pairs)}")

    # current month + 12 ahead
    month_periods = iter_months_current_plus_12(date.today())
    logger.info(f"Month periods (YYYY-MM): {month_periods}")

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
                logger.info(f"Processing route {origin} -> {destination}")

                for dep_month in month_periods:
                    try:
                        created_at = datetime.now(timezone.utc)

                        req = AviasalesRequestParams(
                            token=token,
                            origin=origin,
                            destination=destination,
                            one_way=True,
                            direct=False,
                            limit=31,
                            sorting="price",
                            departure_at=dep_month,  # 'YYYY-MM'
                        )

                        response = collector.collect(req)
                        status_code = 200  # if no exception, we assume OK

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
                            f"Skipping {origin}->{destination} {dep_month} due to error: {str(e)}"
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
            "months": len(month_periods),
        }
    finally:
        client.close()
