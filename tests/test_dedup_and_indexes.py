import pytest
import motor.motor_asyncio
from httpx import AsyncClient
from src.main import app
from src.db.mongo import get_db

@pytest.mark.asyncio
async def test_strict_idempotency():
    """
    TEST: Multiple ingestions should NOT create duplicate documents.
    """
    async with AsyncClient(app=app, base_url="http://test") as ac:
        # First Run
        resp1 = await ac.post("/ingest/run?tenant_id=tenant_a")
        assert resp1.status_code == 200
        
        db = await get_db()
        initial_count = await db.tickets.count_documents({"tenant_id": "tenant_a"})
        assert initial_count > 0
        
        # Second and Third Run (Should not increase count)
        await ac.post("/ingest/run?tenant_id=tenant_a")
        await ac.post("/ingest/run?tenant_id=tenant_a")
        
        final_count = await db.tickets.count_documents({"tenant_id": "tenant_a"})
        assert initial_count == final_count, f"Idempotency Failed: Expected {initial_count}, got {final_count}"

@pytest.mark.asyncio
async def test_indexes_validation():
    """
    TEST: Verify that all required MongoDB indexes are actually created with correct properties.
    """
    db = await get_db()
    indexes = await db.tickets.index_information()
    
    # 1. Unique Index on (tenant_id, external_id)
    unique_exists = False
    for idx_name, idx_spec in indexes.items():
        keys = idx_spec['key']
        if ('tenant_id', 1) in keys and ('external_id', 1) in keys and idx_spec.get('unique'):
            unique_exists = True
            break
    assert unique_exists, "Missing Unique Index on (tenant_id, external_id)"

    # 2. Compound Index on (tenant_id, created_at DESC)
    compound_exists = False
    for idx_name, idx_spec in indexes.items():
        keys = idx_spec['key']
        if ('tenant_id', 1) in keys and ('created_at', -1) in keys:
            compound_exists = True
            break
    assert compound_exists, "Missing Compound Index on (tenant_id, created_at DESC)"

    # 3. TTL Index on created_at
    ttl_exists = False
    for idx_name, idx_spec in indexes.items():
        if 'expireAfterSeconds' in idx_spec:
            ttl_exists = True
            break
    assert ttl_exists, "Missing TTL Index on created_at field"
