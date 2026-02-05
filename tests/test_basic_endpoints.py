import pytest
from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_list_tickets_unauthorized():
    # Should require tenant_id
    response = client.get("/tickets")
    assert response.status_code == 422

def test_ingest_run_endpoint():
    # This should trigger ingestion
    response = client.post("/ingest/run?tenant_id=tenant_a")
    assert response.status_code == 200
    assert "new_tickets" in response.json()
