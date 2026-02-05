"""
Test Task 12: Change detection and data synchronization.

These tests verify that synchronization with the external API behaves correctly.
"""

import pytest
from datetime import datetime, timedelta
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_updated_ticket_is_synced(client: AsyncClient, db):
    """
    When a ticket is updated in the external API, our database should be updated as well.
    """
    from src.services.sync_service import SyncService

    sync_service = SyncService()
    tenant_id = "test_tenant_sync"

    # Create an existing ticket
    old_ticket = {
        "external_id": "ext-001",
        "tenant_id": tenant_id,
        "subject": "Original subject",
        "message": "Original message",
        "created_at": datetime.utcnow() - timedelta(days=1),
        "updated_at": datetime.utcnow() - timedelta(days=1)
    }
    await db.tickets.insert_one(old_ticket)

    # Updated ticket from the external API
    external_ticket = {
        "id": "ext-001",
        "tenant_id": tenant_id,
        "subject": "Updated subject",
        "message": "Updated message",
        "created_at": (datetime.utcnow() - timedelta(days=1)).isoformat() + "Z",
        "updated_at": datetime.utcnow().isoformat() + "Z"  # newer timestamp
    }

    # Run synchronization
    result = await sync_service.sync_ticket(external_ticket, tenant_id)

    # It should be updated
    assert result["action"] == "updated"
    assert "subject" in result["changes"] or "message" in result["changes"]


@pytest.mark.asyncio
async def test_deleted_ticket_soft_delete(client: AsyncClient, db):
    """
    Tickets deleted in the external API should be soft-deleted in our DB.
    """
    from src.services.sync_service import SyncService

    sync_service = SyncService()
    tenant_id = "test_tenant_delete"

    # Create existing tickets
    await db.tickets.insert_many([
        {"external_id": "ext-001", "tenant_id": tenant_id, "subject": "Ticket 1"},
        {"external_id": "ext-002", "tenant_id": tenant_id, "subject": "Ticket 2"},
        {"external_id": "ext-003", "tenant_id": tenant_id, "subject": "Ticket 3"},
    ])

    # In the external API, ext-002 is deleted
    external_ids = ["ext-001", "ext-003"]  # ext-002 is missing

    # Detect deleted tickets
    deleted = await sync_service.detect_deleted_tickets(tenant_id, external_ids)
    assert "ext-002" in deleted

    # Apply soft delete
    count = await sync_service.mark_deleted(tenant_id, deleted)
    assert count == 1

    # Verify in the DB
    ticket = await db.tickets.find_one({"external_id": "ext-002", "tenant_id": tenant_id})
    assert ticket is not None
    assert "deleted_at" in ticket


@pytest.mark.asyncio
async def test_ticket_history_recorded(client: AsyncClient, db):
    """
    Ticket changes should be recorded in the history collection.
    """
    from src.services.sync_service import SyncService

    sync_service = SyncService()
    tenant_id = "test_tenant_history"
    ticket_id = "ext-history-001"

    # Record a history entry
    await sync_service.record_history(
        ticket_id=ticket_id,
        tenant_id=tenant_id,
        action="updated",
        changes={
            "subject": {"old": "Old title", "new": "New title"},
            "status": {"old": "open", "new": "closed"}
        }
    )

    # Fetch history back
    history = await sync_service.get_ticket_history(ticket_id, tenant_id)

    assert len(history) > 0
    assert history[0]["action"] == "updated"
    assert "subject" in history[0]["changes"]


@pytest.mark.asyncio
async def test_ticket_history_api(client: AsyncClient, db):
    """
    Test the ticket history API.
    """
    tenant_id = "test_tenant_api"
    ticket_id = "ext-api-001"

    # Seed a test history document
    await db.ticket_history.insert_one({
        "ticket_id": ticket_id,
        "tenant_id": tenant_id,
        "action": "created",
        "changes": {},
        "recorded_at": datetime.utcnow()
    })

    response = await client.get(
        f"/tickets/{ticket_id}/history",
        params={"tenant_id": tenant_id}
    )
    assert response.status_code == 200

    data = response.json()
    assert data["ticket_id"] == ticket_id
    assert len(data["history"]) > 0


@pytest.mark.asyncio
async def test_unchanged_ticket_not_updated(client: AsyncClient, db):
    """
    Tickets that have not changed must not be updated.
    """
    from src.services.sync_service import SyncService

    sync_service = SyncService()
    tenant_id = "test_tenant_unchanged"

    timestamp = datetime.utcnow() - timedelta(hours=1)

    # Create an existing ticket
    await db.tickets.insert_one({
        "external_id": "ext-unchanged",
        "tenant_id": tenant_id,
        "subject": "Same subject",
        "updated_at": timestamp
    })

    # Attempt sync with the same updated_at
    external_ticket = {
        "id": "ext-unchanged",
        "subject": "Same subject",
        "updated_at": timestamp.isoformat() + "Z"
    }

    result = await sync_service.sync_ticket(external_ticket, tenant_id)

    # No changes expected
    assert result["action"] == "unchanged"
