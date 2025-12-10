from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from routes import health, aviasales


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Log application lifecycle events for easier reload tracking."""
    logger.info("🚀 Starting application...")
    try:
        yield
    finally:
        logger.info("🛑 Stopping application...")


app = FastAPI(title="MD Ticket API", lifespan=lifespan)

routers = [health.router, aviasales.router]


for router in routers:
    app.include_router(router, prefix="/api")
