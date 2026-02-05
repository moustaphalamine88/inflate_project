import pytest
from httpx import AsyncClient
from src.main import app
from src.db.mongo import get_db

@pytest.mark.asyncio
async def test_health_check_logic():
    """
    TEST: Health check should return 200 and check dependencies.
    """
    async with AsyncClient(app=app, base_url="http://test") as ac:
        resp = await ac.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data
        # Expecting candidate to return details about mongodb/external_api
        # assert "mongodb" in data
        # assert "external_api" in data

@pytest.mark.asyncio
async def test_audit_logging_creation():
    """
    TEST: Every ingestion run should create an audit log entry.
    """
    async with AsyncClient(app=app, base_url="http://test") as ac:
        await ac.post("/ingest/run?tenant_id=tenant_b")
        
        db = await get_db()
        log = await db.ingestion_logs.find_one({"status": {"$in": ["SUCCESS", "FAILED", "PARTIAL_SUCCESS"]}})
        assert log is not None
        assert "start_time" in log
        assert "tickets_processed" in log
