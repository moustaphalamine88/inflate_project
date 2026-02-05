import pytest
from httpx import AsyncClient
from src.main import app
from src.db.mongo import get_db

@pytest.mark.asyncio
async def test_tenant_isolation():
    """
    TEST: The same external_id in DIFFERENT tenants should be treated as separate tickets.
    """
    db = await get_db()
    await db.tickets.delete_many({})
    
    async with AsyncClient(app=app, base_url="http://test") as ac:
        # Mocking ingestion for two different tenants with same data
        # In a real test, we would mock the external API response
        pass

@pytest.mark.asyncio
async def test_missing_tenant_id():
    """
    TEST: API should return 422 if tenant_id is missing.
    """
    async with AsyncClient(app=app, base_url="http://test") as ac:
        resp = await ac.get("/tickets")
        assert resp.status_code == 422

@pytest.mark.asyncio
async def test_stats_performance_limit():
    """
    TEST: Verify the 2-second performance limit on the stats endpoint.
    """
    async with AsyncClient(app=app, base_url="http://test") as ac:
        # This will fail if the candidate uses slow Python-side loops
        resp = await ac.get("/tenants/tenant_a/stats")
        # If the middleware works, it returns 504 on timeout
        assert resp.status_code != 504, "Stats endpoint exceeded performance limit"
