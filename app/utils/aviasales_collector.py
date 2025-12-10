from typing import Any, Literal
from datetime import date, datetime, timezone, timedelta

import os
import time

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
    departure_at: date | None = Field(
        None,
        description="Departure date",
    )
    return_at: date | None = Field(
        None,
        description="Return date",
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

    @model_validator(mode="after")
    def _validate_one_way_requires_no_return_at(self) -> "AviasalesRequestParams":
        if self.one_way and self.return_at is not None:
            raise ValueError("For one_way=True, the return_at parameter must be None ")
        return self

    def to_query_dict(self) -> dict:
        """Build query params dict"""
        d = self.model_dump(exclude_none=True, exclude={"token"})

        d["one_way"] = str(self.one_way).lower()
        d["direct"] = str(self.direct).lower()
        d["unique"] = str(self.unique).lower()

        if self.departure_at:
            d["departure_at"] = self.departure_at.isoformat()
        if self.return_at:
            d["return_at"] = self.return_at.isoformat()

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


def collect_calendar_to_mongo(
    *,
    days: int = 30,
) -> dict[str, Any]:
    """Load Aviasales calendar data into MongoDB"""
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

    client = MongoClient(
        host=mongo_host,
        port=mongo_port,
        username=mongo_user,
        password=mongo_password,
    )

    try:
        db = client[mongo_db]
        collection = db["flight_prices_calendar"]

        try:
            req = AviasalesRequestParams(
                token=token,
                origin="MOW",
                destination="NYC",
                one_way=False,
                limit=1000,
                sorting="price",
            )
        except Exception as e:
            logger.error(f"Invalid request parameters in Aviasales: {e}")
            raise

        number_tickets: int = 0
        number_missed: int = 0
        today = date.today()
        data_range = [today + timedelta(days=x) for x in range(days)]

        with AviasalesFlightCollector() as collector:
            for idx, dep_date in enumerate(data_range):
                try:
                    req.departure_at = dep_date
                    response = collector.collect(req)
                    number_tickets += len(response.get("data", []))

                    filter_doc = {
                        "origin": req.origin,
                        "destination": req.destination,
                        "departure_at": dep_date.isoformat(),
                    }

                    update_doc = {
                        "$set": {
                            **response,
                            "created_at": datetime.now(timezone.utc).isoformat(),
                        }
                    }

                    collection.update_one(filter_doc, update_doc, upsert=True)

                except Exception as e:
                    number_missed += 1
                    logger.exception(f"Skipping {dep_date} due to error: {str(e)}")

                logger.success(
                    f"Response {idx}: Collected {number_tickets} offers (missed={number_missed})."
                )

        return {
            "number_tickets": number_tickets,
            "number_missed": number_missed,
            "collection": "flight_prices_calendar",
            "db": mongo_db,
        }
    finally:
        client.close()
