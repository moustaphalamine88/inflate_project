"""
Task 12: Data synchronization service.

Responsible for synchronizing ticket data with the external API.

Requirements:
1. Use the ticket `updated_at` field to update only tickets that have changed.
2. Apply soft delete (`deleted_at` field) for tickets deleted in the external system.
3. Record change history in the `ticket_history` collection.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from src.db.mongo import get_db


class SyncService:
    """
    Data synchronization service.
    """

    HISTORY_COLLECTION = "ticket_history"

    async def sync_ticket(self, external_ticket: dict, tenant_id: str) -> dict:
        """
        Synchronize a single ticket.

        Args:
            external_ticket: Ticket payload from the external API.
            tenant_id: Tenant identifier.

        Returns:
            {
                "action": "created" | "updated" | "unchanged",
                "ticket_id": str,
                "changes": List[str]  # list of changed fields
            }

        TODO: Implement:
        - Look up the existing ticket (by tenant_id + external_id).
        - Compare updated_at to decide whether an update is needed.
        - If changed, update the ticket and record history.
        """
        # TODO: implement
        return {
            "action": "unchanged",
            "ticket_id": external_ticket.get("id"),
            "changes": []
        }

    async def mark_deleted(self, tenant_id: str, external_ids: List[str]) -> int:
        """
        Handle tickets that were deleted in the external system (soft delete).

        Args:
            tenant_id: Tenant identifier.
            external_ids: List of external ticket IDs that have been deleted.

        Returns:
            Number of tickets that were soft-deleted.

        TODO: Implement:
        - Set the `deleted_at` field.
        - Record a history entry.
        """
        # TODO: implement
        return 0

    async def detect_deleted_tickets(self, tenant_id: str, external_ids: List[str]) -> List[str]:
        """
        Detect tickets that appear to have been deleted externally.

        Finds tickets that exist in our DB but are missing from `external_ids`.

        Args:
            tenant_id: Tenant identifier.
            external_ids: Complete list of ticket IDs from the external API.

        Returns:
            List of external IDs that are presumed deleted.

        TODO: Implement this query.
        """
        # TODO: implement
        return []

    async def record_history(
        self,
        ticket_id: str,
        tenant_id: str,
        action: str,
        changes: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Record a change history entry.

        Args:
            ticket_id: Ticket identifier.
            tenant_id: Tenant identifier.
            action: "created" | "updated" | "deleted".
            changes: Change details (field -> {old: ..., new: ...}).

        Returns:
            ID of the created history document.
        """
        db = await get_db()

        history_doc = {
            "ticket_id": ticket_id,
            "tenant_id": tenant_id,
            "action": action,
            "changes": changes or {},
            "recorded_at": datetime.utcnow()
        }

        result = await db[self.HISTORY_COLLECTION].insert_one(history_doc)
        return str(result.inserted_id)

    async def get_ticket_history(
        self,
        ticket_id: str,
        tenant_id: str,
        limit: int = 50
    ) -> List[dict]:
        """
        Retrieve ticket change history.

        Args:
            ticket_id: Ticket identifier.
            tenant_id: Tenant identifier.
            limit: Maximum number of records to return.

        Returns:
            List of history entries in reverse chronological order.
        """
        db = await get_db()

        cursor = db[self.HISTORY_COLLECTION].find(
            {"ticket_id": ticket_id, "tenant_id": tenant_id}
        ).sort("recorded_at", -1).limit(limit)

        return await cursor.to_list(length=limit)

    def compute_changes(self, old_doc: dict, new_doc: dict, fields: List[str]) -> Dict[str, Any]:
        """
        Compute field-level differences between two documents.

        Args:
            old_doc: Previous version of the document.
            new_doc: New version of the document.
            fields: List of fields to compare.

        Returns:
            A mapping of changed fields to their before/after values:
            {
                "field_name": {"old": ..., "new": ...},
                ...
            }
        """
        changes = {}

        for field in fields:
            old_value = old_doc.get(field)
            new_value = new_doc.get(field)

            if old_value != new_value:
                changes[field] = {
                    "old": old_value,
                    "new": new_value
                }

        return changes
