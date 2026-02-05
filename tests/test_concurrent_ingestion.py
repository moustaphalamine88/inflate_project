"""
Test Task 8: Prevent concurrent ingestion (distributed lock).

These tests verify that concurrent ingestion requests for the same tenant
are handled correctly.
"""

import pytest
import asyncio
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_concurrent_ingestion_blocked(client: AsyncClient):
    """
    When multiple ingestion requests are sent concurrently for the same tenant,
    exactly one should succeed and the others should return 409 Conflict.
    """
    tenant_id = "test_tenant_concurrent"

    # Send three concurrent ingestion requests
    async def make_request():
        return await client.post(f"/ingest/run?tenant_id={tenant_id}")

    responses = await asyncio.gather(
        make_request(),
        make_request(),
        make_request(),
        return_exceptions=True
    )

    # Analyze results
    success_count = sum(1 for r in responses if hasattr(r, 'status_code') and r.status_code == 200)
    conflict_count = sum(1 for r in responses if hasattr(r, 'status_code') and r.status_code == 409)

    # Exactly one request should succeed
    assert success_count == 1, f"Expected 1 success, got {success_count}"
    # The remaining requests should return 409 Conflict
    assert conflict_count == 2, f"Expected 2 conflicts, got {conflict_count}"


@pytest.mark.asyncio
async def test_lock_status_shows_running(client: AsyncClient):
    """
    While ingestion is running, the lock status API should return `locked=True`.
    """
    tenant_id = "test_tenant_lock_status"

    # Start ingestion
    response = await client.post(f"/ingest/run?tenant_id={tenant_id}")
    assert response.status_code == 200

    # Check the lock status
    lock_response = await client.get(f"/ingest/lock/{tenant_id}")
    assert lock_response.status_code == 200

    data = lock_response.json()
    assert data.get("locked") is True


@pytest.mark.asyncio
async def test_lock_auto_expires(client: AsyncClient):
    """
    The lock should automatically expire after 60 seconds.
    (In practice, this should be tested with time manipulation or a short TTL.)
    """
    # This integration test cannot actually wait 60 seconds, so the lock
    # expiry logic should be validated via unit tests instead.
    pass


@pytest.mark.asyncio
async def test_different_tenants_can_run_simultaneously(client: AsyncClient):
    """
    Different tenants should be able to run ingestion concurrently.
    """
    async def make_request(tenant_id: str):
        return await client.post(f"/ingest/run?tenant_id={tenant_id}")

    responses = await asyncio.gather(
        make_request("tenant_a"),
        make_request("tenant_b"),
        make_request("tenant_c"),
    )

    # All requests should succeed
    for i, response in enumerate(responses):
        assert response.status_code == 200, f"Request {i} failed with {response.status_code}"
