from src.db.mongo import get_db
import pymongo


async def create_indexes():
    """
    Create MongoDB indexes required for common query patterns and to keep
    the dataset manageable over time (e.g. compound indexes, unique
    constraints, TTL on old data).
    """
    db = await get_db()
    tickets = db.tickets

     # 🐛 DEBUG TASK E: Inefficient indexes
    # The indexes below are intentionally misaligned with real query
    # patterns and will cause performance issues.
    # ============================================================

    # 🐛 Issue 1: Index on created_at without tenant_id
    # Most queries filter by tenant_id, so this index is rarely used.
    await tickets.create_index([("created_at", pymongo.ASCENDING)])

    # 🐛 Issue 2: Single-field index on a low-cardinality field
    # status has only three values (open/closed/pending), so selectivity is low.
    await tickets.create_index([("status", pymongo.ASCENDING)])

    # 🐛 Issue 3: Single-field index on urgency (also low cardinality)
    await tickets.create_index([("urgency", pymongo.ASCENDING)])

     # 🐛 Issue 4: Wrong order in composite index
    # Queries typically filter by tenant_id and then sort by created_at,
    # but this index uses the reverse order.
    await tickets.create_index([
        ("created_at", pymongo.DESCENDING),
        ("tenant_id", pymongo.ASCENDING)
    ])

    # 🐛 Issue 5: Missing unique index for idempotency
    # The (tenant_id, external_id) pair should be unique to prevent duplicates.

    # ingestion_jobs 컬렉션 인덱스
    ingestion_jobs = db.ingestion_jobs
    await ingestion_jobs.create_index([("tenant_id", pymongo.ASCENDING)])
    await ingestion_jobs.create_index([("status", pymongo.ASCENDING)])

    # ingestion_logs 컬렉션 인덱스
    ingestion_logs = db.ingestion_logs
    await ingestion_logs.create_index([("tenant_id", pymongo.ASCENDING)])
    await ingestion_logs.create_index([("job_id", pymongo.ASCENDING)])


# ============================================================
# Hint: Example of good index design
# ============================================================
# The commented-out indexes below illustrate better patterns.
# To address Debug Task E, replace the inefficient indexes above
# with indexes that follow these patterns.
#
# # Unique index for idempotency
# await tickets.create_index(
#     [("tenant_id", pymongo.ASCENDING), ("external_id", pymongo.ASCENDING)],
#     unique=True
# )
#
# # Efficient composite index (tenant_id first, then created_at)
# await tickets.create_index([
#     ("tenant_id", pymongo.ASCENDING),
#     ("created_at", pymongo.DESCENDING)
# ])
#
# # Composite index for multi-condition queries
# await tickets.create_index([
#     ("tenant_id", pymongo.ASCENDING),
#     ("status", pymongo.ASCENDING),
#     ("created_at", pymongo.DESCENDING)
# ])
#
# # TTL index (automatic cleanup of old data)
# await tickets.create_index(
#     [("created_at", pymongo.ASCENDING)],
#     expireAfterSeconds=60 * 60 * 24 * 90  # 90 days
# )
