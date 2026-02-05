# src/db/mongo.py
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from src.core.config import settings

_client: Optional[AsyncIOMotorClient] = None
_db: Optional[AsyncIOMotorDatabase] = None


def _get_db_name() -> str:
    """
    The starter-kit may name the DB setting differently depending on version.
    This helper makes the code resilient.
    """
    for attr in ("MONGO_DB_NAME", "MONGO_DATABASE", "DB_NAME", "MONGODB_DB", "MONGO_DB"):
        if hasattr(settings, attr):
            return getattr(settings, attr)

    # Fallback: some projects embed the db name inside the URL.
    # If nothing is found, default to "tickets" (you can change).
    return "tickets"


async def init_mongo() -> None:
    """
    Initialize one global Mongo client for the whole process (connection pool).
    Called once at FastAPI startup.
    """
    global _client, _db
    if _client is None:
        _client = AsyncIOMotorClient(
            settings.MONGO_URL,
            maxPoolSize=50,
            minPoolSize=5,
            serverSelectionTimeoutMS=3000,
            connectTimeoutMS=3000,
        )
        _db = _client[_get_db_name()]


async def close_mongo() -> None:
    """Close the global Mongo client on shutdown."""
    global _client, _db
    if _client is not None:
        _client.close()
    _client = None
    _db = None


async def get_db() -> AsyncIOMotorDatabase:
    """Return the cached database handle."""
    if _db is None:
        await init_mongo()
    return _db
