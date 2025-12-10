from datetime import timedelta
import os


default_args = {
    "owner": "Nikolay Teplyakov",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


AVIASALES_SERVICE_URL = os.getenv(
    "AVIASALES_SERVICE_URL",
    "http://backend_app:8000",
)
