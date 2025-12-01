from fastapi import APIRouter, Response

from utils.db_check import check_mongodb, check_postgres

router = APIRouter()


@router.get("/health")
async def health_check(response: Response) -> dict[str, str | dict]:
    """Health check endpoint with database connectivity validation."""
    postgres_ok = check_postgres()
    mongodb_ok = check_mongodb()

    if postgres_ok and mongodb_ok:
        response.status_code = 200
        return {
            "status": "healthy",
            "databases": {"postgres": "ok", "mongodb": "ok"},
        }
    else:
        response.status_code = 503
        return {
            "status": "unhealthy",
            "databases": {
                "postgres": "ok" if postgres_ok else "error",
                "mongodb": "ok" if mongodb_ok else "error",
            },
        }
