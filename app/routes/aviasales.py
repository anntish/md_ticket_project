from fastapi import APIRouter, HTTPException
from loguru import logger

from utils.aviasales_collector import collect_calendar_to_mongo

router = APIRouter()


@router.post("/aviasales/collect")
async def trigger_aviasales_collection(one_way: bool) -> dict:
    """
    Get Aviasales calendar data and load it into MongoDB
    """
    try:
        result = collect_calendar_to_mongo(one_way=one_way)
        return {"status": "ok", "result": result}
    except Exception as exc:
        logger.exception(f"Error in Aviasales collection: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
