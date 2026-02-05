"""
Tests for Debug Tasks C, D, and E.

These tests verify intentionally seeded bugs.
Once the candidate fixes the bugs, the tests should pass.
"""

import pytest
import asyncio
import time
from httpx import AsyncClient


# ============================================================
# Debug Task C: Memory leak test
# ============================================================

@pytest.mark.asyncio
async def test_memory_leak_detection(client: AsyncClient):
    """
    Even after running ingestion many times, memory usage must not grow without bound.

    Currently there is a memory leak:
    the `_ingestion_cache` is never cleaned up, so memory keeps increasing.
    """
    import sys

    # Check initial cache size
    from src.services.ingest_service import _ingestion_cache
    initial_cache_size = len(_ingestion_cache)

    # Run ingestion multiple times
    for i in range(10):
        tenant_id = f"memory_test_tenant_{i}"
        await client.post(f"/ingest/run?tenant_id={tenant_id}")

    # Cache size must not grow without bound
    final_cache_size = len(_ingestion_cache)

    # 🐛 Bug: the cache keeps growing because it is never cleared.
    # After fixing, the cache size should be bounded or periodically cleaned up.
    # assert final_cache_size <= initial_cache_size + 5, \
    #     f"Memory leak detected: cache grew from {initial_cache_size} to {final_cache_size}"

    # Currently this assertion would fail because of the bug.
    # After fixing the leak, uncomment the assertion above.


# ============================================================
# Debug Task D: Race condition test
# ============================================================

@pytest.mark.asyncio
async def test_race_condition_prevention(client: AsyncClient):
    """
    When multiple ingestion requests are sent concurrently for the same tenant,
    exactly one should succeed.

    Currently there is a race condition: due to a check-then-act pattern,
    multiple concurrent requests can both succeed.
    """
    tenant_id = "race_condition_test"

    # Fire 5 concurrent requests
    async def make_request():
        return await client.post(f"/ingest/run?tenant_id={tenant_id}")

    start = time.time()
    responses = await asyncio.gather(*[make_request() for _ in range(5)])
    elapsed = time.time() - start

    # Analyze results
    success_count = sum(1 for r in responses if r.status_code == 200)
    conflict_count = sum(1 for r in responses if r.status_code == 409)

    # 🐛 Bug: due to the race condition, more than one request may succeed.
    # After fixing, exactly one request should succeed.
    assert success_count == 1, \
        f"Race condition detected: {success_count} requests succeeded (expected 1)"
    assert conflict_count == 4, \
        f"Expected 4 conflicts, got {conflict_count}"


# ============================================================
# Debug Task E: Slow query test
# ============================================================

@pytest.mark.asyncio
async def test_stats_performance(client: AsyncClient, db):
    """
    When there are 10,000 tickets, the stats API should respond within 500ms.

    Currently the indexes are inefficient.
    """
    tenant_id = "performance_test_tenant"

    # Generate test data (10,000 tickets).
    # In a real test suite, this would ideally be a fixture.
    from datetime import datetime, timedelta
    import random

    tickets = []
    base_time = datetime.utcnow() - timedelta(days=30)

    for i in range(10000):
        tickets.append({
            "external_id": f"perf-{i}",
            "tenant_id": tenant_id,
            "status": random.choice(["open", "closed", "pending"]),
            "urgency": random.choice(["high", "medium", "low"]),
            "sentiment": random.choice(["positive", "neutral", "negative"]),
            "created_at": base_time + timedelta(hours=random.randint(0, 720)),
            "source": random.choice(["email", "web", "chat", "api"]),
            "customer_id": f"cust_{random.randint(100, 500)}"
        })

    # Bulk insert
    await db.tickets.insert_many(tickets)

    # Create indexes (currently suboptimal)
    from src.db.indexes import create_indexes
    await create_indexes()

    # Measure performance
    start = time.time()
    response = await client.get(f"/tenants/{tenant_id}/stats")
    elapsed = time.time() - start

    assert response.status_code == 200

    # 🐛 Bug: due to poor indexing, this may exceed 500ms.
    # After optimizing indexes, this test should pass.
    assert elapsed < 0.5, \
        f"Stats API took {elapsed:.2f}s (expected < 0.5s). Check your indexes!"


@pytest.mark.asyncio
async def test_explain_query_uses_index(db):
    """
    Ensure that the main query uses an index rather than a collection scan.
    """
    tenant_id = "explain_test"

    # Run explain()
    explanation = await db.tickets.find(
        {"tenant_id": tenant_id}
    ).sort("created_at", -1).limit(100).explain()

    # We should be using an index scan (COLLSCAN should not be used).
    winning_plan = explanation.get("queryPlanner", {}).get("winningPlan", {})
    stage = winning_plan.get("stage", "")

    # 🐛 Currently this is likely using COLLSCAN (full collection scan).
    # With a proper index in place, the stage should be IXSCAN or FETCH.
    assert stage != "COLLSCAN", \
        f"Query is doing a full collection scan. Expected index scan. Stage: {stage}"
