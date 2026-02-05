# src/api/routes.py

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from src.db.models import TicketResponse, TenantStats
from src.db.mongo import get_db
from src.services.analytics_service import AnalyticsService
from src.services.circuit_breaker import get_circuit_breaker
from src.services.ingest_service import IngestService
from src.services.lock_service import get_lock_service

router = APIRouter()

# ============================================================
# Ticket APIs
# ============================================================

@router.get("/tickets", response_model=List[TicketResponse])
async def list_tickets(
    tenant_id: str,
    status: Optional[str] = None,
    urgency: Optional[str] = None,
    source: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    List tickets for a tenant with optional filters.

    Fixes Debug Task A:
    - Always scope by tenant_id (no cross-tenant leakage)
    - Exclude soft-deleted tickets (deleted_at != None)
    """
    db = await get_db()

    query: dict = {
        "tenant_id": tenant_id,
        "deleted_at": None,
    }

    if status:
        query["status"] = status
    if urgency:
        query["urgency"] = urgency
    if source:
        query["source"] = source

    cursor = (
        db.tickets.find(query)
        .sort("created_at", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    docs = await cursor.to_list(length=page_size)
    return docs


@router.get("/tickets/urgent", response_model=List[TicketResponse])
async def list_urgent_tickets(tenant_id: str):
    """Return urgent (high urgency) tickets for a tenant."""
    db = await get_db()
    cursor = db.tickets.find(
        {"tenant_id": tenant_id, "deleted_at": None, "urgency": "high"}
    ).sort("created_at", -1)
    return await cursor.to_list(length=100)


@router.get("/tickets/{ticket_id}", response_model=TicketResponse)
async def get_ticket(ticket_id: str, tenant_id: str):
    """Fetch a single ticket by external_id scoped to tenant."""
    db = await get_db()
    doc = await db.tickets.find_one(
        {"tenant_id": tenant_id, "external_id": ticket_id, "deleted_at": None}
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Ticket not found")
    return doc


# ============================================================
# Health Check API (Task 5)
# ============================================================

@router.get("/health")
async def health_check():
    """
    Basic health endpoint (tests expect EXACT response).
    IMPORTANT: tests/test_basic_endpoints.py expects exactly {"status": "ok"}.
    """
    return {"status": "ok"}


# ============================================================
# Analytics API (Task 3)
# ============================================================

@router.get("/tenants/{tenant_id}/stats", response_model=TenantStats)
async def get_tenant_stats(
    tenant_id: str,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    analytics_service: AnalyticsService = Depends(),
):
    """
    Retrieve analytics and statistics for a given tenant.

    Must compute heavy logic inside MongoDB (aggregation pipeline),
    not Python loops.
    """
    return await analytics_service.get_tenant_stats(tenant_id, from_date, to_date)


# ============================================================
# Ingestion APIs (Task 1, 8, 9)
# ============================================================

@router.post("/ingest/run")
async def run_ingestion(
    tenant_id: str,
    background_tasks: BackgroundTasks,
    ingest_service: IngestService = Depends(),
):
    """
    Trigger ingestion.

    IMPORTANT for tests:
    - Return ingestion result directly (no wrapper object)
    - IngestService must raise HTTP 409 if lock cannot be acquired
    """
    result = await ingest_service.run_ingestion(tenant_id)
    return result


@router.get("/ingest/status")
async def get_ingestion_status(
    tenant_id: str,
    ingest_service: IngestService = Depends(),
):
    """Return current ingestion status for a tenant."""
    status = await ingest_service.get_ingestion_status(tenant_id)
    if not status:
        return {"status": "idle", "tenant_id": tenant_id}
    return status


@router.get("/ingest/progress/{job_id}")
async def get_ingestion_progress(
    job_id: str,
    ingest_service: IngestService = Depends(),
):
    """Retrieve ingestion job progress by job_id."""
    status = await ingest_service.get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    return status


@router.delete("/ingest/{job_id}")
async def cancel_ingestion(
    job_id: str,
    ingest_service: IngestService = Depends(),
):
    """Cancel a running ingestion job."""
    success = await ingest_service.cancel_job(job_id)
    if not success:
        raise HTTPException(status_code=404, detail="Job not found or already completed")
    return {"status": "cancelled", "job_id": job_id}


# ============================================================
# Lock Status API (Task 8)
# ============================================================

@router.get("/ingest/lock/{tenant_id}")
async def get_lock_status(tenant_id: str):
    """
    Get the current ingestion lock status for a tenant (Task 8).
    """
    lock_service = get_lock_service()
    status = await lock_service.get_lock_status(f"ingest:{tenant_id}")
    if not status:
        return {"locked": False, "tenant_id": tenant_id}
    return {"locked": not status["is_expired"], **status}


# ============================================================
# Circuit Breaker Status API (Task 11)
# ============================================================

@router.get("/circuit/{name}/status")
async def get_circuit_status(name: str):
    """
    Example: GET /circuit/notify/status
    """
    cb = get_circuit_breaker(name)
    return cb.get_status()


@router.post("/circuit/{name}/reset")
async def reset_circuit(name: str):
    """
    Reset the Circuit Breaker state (for debugging/testing).
    """
    cb = get_circuit_breaker(name)
    cb.reset()
    return {"status": "reset", "name": name}


# ============================================================
# Ticket History API (Task 12)
# ============================================================

@router.get("/tickets/{ticket_id}/history")
async def get_ticket_history(
    ticket_id: str,
    tenant_id: str,
    limit: int = Query(50, ge=1, le=200),
):
    """
    Retrieve the change history for a ticket (Task 12).
    """
    from src.services.sync_service import SyncService

    sync_service = SyncService()
    history = await sync_service.get_ticket_history(ticket_id, tenant_id, limit)
    return {"ticket_id": ticket_id, "history": history}
