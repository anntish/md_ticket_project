from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from loguru import logger
from models import AviasalesRequestParams
import time


class RateLimitRespectingHTTPAdapter(HTTPAdapter):
    """
    HTTPAdapter with Smart 429 Processing Too Many Requests:
    - If the Retry-After header has arrived, we are waiting for the specified time.
    - If not, we use a fallback of 60 seconds (for minute-based limits, as in /v3/prices_for_dates).

    All other errors (5xx, connection loss) are handled via standard Retry.
    """

    def send(self, request, **kwargs):
        response = super().send(request, **kwargs)

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                delay = int(retry_after)
                logger.warning(f"429: Retry-After={delay}s → sleep...")
            else:
                delay = 60
                logger.warning("429: Retry-After отсутствует → fallback на 60s")

            eps = 0.5
            time.sleep(delay + eps)

            response = super().send(request, **kwargs)

        return response


class AviasalesFlightCollector:
    """Ticket Data Collector"""

    _API_URL = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"

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

    def _enforce_rate_limit(self):
        """Ensures that at least min_delay seconds have passed between requests."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
        self._last_request_time = time.monotonic()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    def collect(self, params: AviasalesRequestParams) -> dict[str, Any]:
        """
        Executes an API request with validated parameters.
        """
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
                logger.warning(f"⚠Rate limit low: {remaining}/min left")

            data: dict[str, Any] = response.json()

            if not data.get("success"):
                err = data.get("error") or "Неизвестная ошибка API"
                logger.error(f"API error: {err}")
                raise RuntimeError(f"Aviasales API error: {err}")

            return data

        except Exception as e:
            logger.exception(f"❌ Ошибка при выполнении запроса {str(e)}")
            raise
