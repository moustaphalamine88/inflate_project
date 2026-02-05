"""
Task 8: Distributed lock service.

Implement a distributed lock using MongoDB atomic operations.
Do not use external distributed lock libraries (redis-lock, pottery, etc.).

Requirements:
1. Prevent concurrent ingestion for the same tenant.
2. Return 409 Conflict when lock acquisition fails (handled by IngestService).
3. Automatically release locks when they are not refreshed within 60 seconds.
4. Provide lock status inspection APIs.
"""

from datetime import datetime, timedelta
from typing import Optional

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from src.db.mongo import get_db


class LockService:
    """
    MongoDB-based distributed lock service.

    We store documents like:
    {
      "resource_id": "ingest:tenant_a",
      "owner_id": "job_id",
      "acquired_at": datetime,
      "expires_at": datetime
    }

    IMPORTANT:
    - Ensure a UNIQUE index on resource_id to prevent duplicates under race.
    - We create indexes lazily (once per process).
    """

    LOCK_COLLECTION = "distributed_locks"
    LOCK_TTL_SECONDS = 60

    _indexes_ready: bool = False

    async def _ensure_indexes(self) -> None:
        if LockService._indexes_ready:
            return

        db = await get_db()
        col = db[self.LOCK_COLLECTION]

        # Unique lock per resource_id.
        await col.create_index([("resource_id", 1)], unique=True)

        # Optional: TTL index can auto-clean expired locks.
        # Note: TTL deletes are not instant; they run on a background interval.
        await col.create_index([("expires_at", 1)], expireAfterSeconds=0)

        LockService._indexes_ready = True

    async def acquire_lock(self, resource_id: str, owner_id: str) -> bool:
        """
        Attempt to acquire a lock atomically.

        Logic:
        - If missing OR expired -> acquire.
        - If already owned by same owner -> refresh (re-entrant).
        - If owned by someone else and not expired -> fail.

        Returns:
            True if lock acquired/refreshed by owner_id, else False.
        """
        await self._ensure_indexes()
        db = await get_db()
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=self.LOCK_TTL_SECONDS)

        col = db[self.LOCK_COLLECTION]

        try:
            doc = await col.find_one_and_update(
                filter={
                    "resource_id": resource_id,
                    "$or": [
                        {"expires_at": {"$lte": now}},  # expired -> takeover allowed
                        {"owner_id": owner_id},         # re-entrant refresh
                        {"expires_at": {"$exists": False}},
                    ],
                },
                update={
                    "$set": {
                        "resource_id": resource_id,
                        "owner_id": owner_id,
                        "acquired_at": now,
                        "expires_at": expires_at,
                    }
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )

            return bool(doc) and doc.get("owner_id") == owner_id

        except DuplicateKeyError:
            # Another contender created the lock doc at the same time.
            return False

    async def release_lock(self, resource_id: str, owner_id: str) -> bool:
        """
        Release the lock ONLY if we are the current owner.
        """
        await self._ensure_indexes()
        db = await get_db()

        result = await db[self.LOCK_COLLECTION].delete_one(
            {"resource_id": resource_id, "owner_id": owner_id}
        )
        return result.deleted_count > 0

    async def refresh_lock(self, resource_id: str, owner_id: str) -> bool:
        """
        Extend the lock expiration if still owned by owner_id.
        """
        await self._ensure_indexes()
        db = await get_db()

        now = datetime.utcnow()
        new_expires = now + timedelta(seconds=self.LOCK_TTL_SECONDS)

        result = await db[self.LOCK_COLLECTION].update_one(
            filter={
                "resource_id": resource_id,
                "owner_id": owner_id,
                "expires_at": {"$gt": now},
            },
            update={"$set": {"expires_at": new_expires}},
        )
        return result.modified_count > 0

    async def get_lock_status(self, resource_id: str) -> Optional[dict]:
        """
        Get current lock status for a resource.
        """
        db = await get_db()
        lock = await db[self.LOCK_COLLECTION].find_one({"resource_id": resource_id})
        if not lock:
            return None

        now = datetime.utcnow()
        expires_at = lock.get("expires_at", now)

        return {
            "resource_id": lock["resource_id"],
            "owner_id": lock["owner_id"],
            "acquired_at": lock.get("acquired_at"),
            "expires_at": expires_at,
            "is_expired": now > expires_at,
        }


# Optional singleton accessor (safe & convenient)
_lock_service: Optional[LockService] = None


def get_lock_service() -> LockService:
    global _lock_service
    if _lock_service is None:
        _lock_service = LockService()
    return _lock_service
