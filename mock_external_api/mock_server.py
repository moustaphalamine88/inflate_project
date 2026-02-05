import json
import random
import asyncio
import time
from fastapi import FastAPI, Query, HTTPException, Request, Response
from typing import Optional, List
from pydantic import BaseModel
import os
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Mock External Services")

# Load seed data
DATA_PATH = os.path.join(os.path.dirname(__file__), "data/seed_tickets.json")
with open(DATA_PATH, "r") as f:
    TICKETS = json.load(f)

# ============================================================
# Rate Limiting (Task 10)
# - Limit to 60 requests per minute
# - If exceeded, return 429 with a Retry-After header
# ============================================================
RATE_LIMIT_REQUESTS = 60  # requests per minute
RATE_LIMIT_WINDOW = 60    # seconds

request_timestamps: List[float] = []

def check_rate_limit() -> tuple[bool, int]:
    """
    Check if rate limit is exceeded.
    Returns (is_allowed, retry_after_seconds)
    """
    global request_timestamps
    now = time.time()

    # Remove timestamps older than the window
    request_timestamps = [ts for ts in request_timestamps if now - ts < RATE_LIMIT_WINDOW]

    if len(request_timestamps) >= RATE_LIMIT_REQUESTS:
        # Calculate retry-after: time until oldest request expires
        oldest = min(request_timestamps)
        retry_after = int(RATE_LIMIT_WINDOW - (now - oldest)) + 1
        return False, retry_after

    request_timestamps.append(now)
    return True, 0


# ============================================================
# Simulated data changes (Task 12)
# - A subset of tickets are treated as "deleted"
# - A subset of tickets are treated as "modified"
# ============================================================
# Roughly 5% of tickets are flagged as deleted
DELETED_TICKET_IDS = set(random.sample([t["id"] for t in TICKETS], k=len(TICKETS) // 20))

# Roughly 10% of tickets are flagged as modified (updated_at > created_at)
MODIFIED_TICKET_IDS = set(random.sample(
    [t["id"] for t in TICKETS if t["id"] not in DELETED_TICKET_IDS],
    k=len(TICKETS) // 10
))


@app.get("/external/support-tickets")
async def get_external_tickets(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    include_deleted: bool = Query(False)
):
    # Rate limit check
    is_allowed, retry_after = check_rate_limit()
    if not is_allowed:
        logger.warning(f"Rate limit exceeded. Retry after {retry_after}s")
        raise HTTPException(
            status_code=429,
            detail="Too Many Requests",
            headers={"Retry-After": str(retry_after)}
        )

    # Filter out deleted tickets unless explicitly requested
    if include_deleted:
        available_tickets = TICKETS
    else:
        available_tickets = [t for t in TICKETS if t["id"] not in DELETED_TICKET_IDS]

    start = (page - 1) * page_size
    end = start + page_size

    subset = available_tickets[start:end]
    next_page = page + 1 if end < len(available_tickets) else None

    # Add updated_at field and deleted flag
    enriched_tickets = []
    for ticket in subset:
        enriched = ticket.copy()

        # Add updated_at field
        if ticket["id"] in MODIFIED_TICKET_IDS:
            # Modified tickets have updated_at after created_at
            from datetime import datetime, timedelta
            created = datetime.fromisoformat(ticket["created_at"].replace("Z", "+00:00"))
            updated = created + timedelta(hours=random.randint(1, 48))
            enriched["updated_at"] = updated.isoformat().replace("+00:00", "Z")
        else:
            enriched["updated_at"] = ticket["created_at"]

        enriched_tickets.append(enriched)

    return {
        "tickets": enriched_tickets,
        "next_page": next_page,
        "total_count": len(available_tickets)
    }


@app.get("/external/support-tickets/{ticket_id}")
async def get_single_ticket(ticket_id: str):
    """Fetch a single ticket (used for change detection)."""
    # Rate limit check
    is_allowed, retry_after = check_rate_limit()
    if not is_allowed:
        raise HTTPException(
            status_code=429,
            detail="Too Many Requests",
            headers={"Retry-After": str(retry_after)}
        )

    for ticket in TICKETS:
        if ticket["id"] == ticket_id:
            if ticket_id in DELETED_TICKET_IDS:
                raise HTTPException(status_code=404, detail="Ticket deleted")

            enriched = ticket.copy()
            if ticket_id in MODIFIED_TICKET_IDS:
                from datetime import datetime, timedelta
                created = datetime.fromisoformat(ticket["created_at"].replace("Z", "+00:00"))
                updated = created + timedelta(hours=random.randint(1, 48))
                enriched["updated_at"] = updated.isoformat().replace("+00:00", "Z")
                # Simulate content change
                enriched["message"] = enriched["message"] + " [UPDATED]"
            else:
                enriched["updated_at"] = ticket["created_at"]

            return enriched

    raise HTTPException(status_code=404, detail="Ticket not found")


@app.get("/external/deleted-tickets")
async def get_deleted_ticket_ids():
    """Fetch the list of deleted ticket IDs (for synchronization)."""
    # Rate limit check
    is_allowed, retry_after = check_rate_limit()
    if not is_allowed:
        raise HTTPException(
            status_code=429,
            detail="Too Many Requests",
            headers={"Retry-After": str(retry_after)}
        )

    return {"deleted_ids": list(DELETED_TICKET_IDS)}


class Notification(BaseModel):
    ticket_id: str
    tenant_id: str
    urgency: str
    reason: str


# Track attempts per ticket_id to force retry logic
notification_attempts: dict = {}

# Circuit breaker simulation - after consecutive failures, reject all requests for a period
circuit_open_until: float = 0
CIRCUIT_FAILURE_THRESHOLD = 10  # open the circuit after 10 consecutive failures
consecutive_failures = 0


@app.post("/notify")
async def notify(notification: Notification):
    global consecutive_failures, circuit_open_until

    ticket_id = notification.ticket_id
    attempts = notification_attempts.get(ticket_id, 0) + 1
    notification_attempts[ticket_id] = attempts

    # Simulate variable latency
    await asyncio.sleep(random.uniform(0.1, 0.5))

    # Circuit breaker simulation
    if time.time() < circuit_open_until:
        logger.warning(f"Circuit OPEN - rejecting notification for {ticket_id}")
        raise HTTPException(status_code=503, detail="Service Unavailable - Circuit Open")

    # DETERMINISTIC FAILURE: Fails on 1st and 2nd attempt, succeeds on 3rd
    if attempts < 3:
        consecutive_failures += 1

        # Open circuit if too many consecutive failures
        if consecutive_failures >= CIRCUIT_FAILURE_THRESHOLD:
            circuit_open_until = time.time() + 30  # keep circuit open for 30 seconds
            logger.warning(f"Circuit OPENED for 30 seconds due to {consecutive_failures} consecutive failures")
            consecutive_failures = 0

        logger.info(f"Notification FAILED for {ticket_id} (Attempt: {attempts})")
        raise HTTPException(status_code=500, detail=f"Service Unavailable (Attempt {attempts})")

    # Success - reset consecutive failures
    consecutive_failures = 0

    logger.info(f"Notification SUCCESS for {ticket_id} (Attempt: {attempts})")
    return {"status": "sent", "ticket_id": ticket_id, "attempts": attempts}


@app.get("/health")
async def health():
    """Mock API health check."""
    return {"status": "ok", "service": "mock-external-api"}


@app.get("/rate-limit/status")
async def rate_limit_status():
    """Inspect current rate limit state (for debugging)."""
    now = time.time()
    active_requests = len([ts for ts in request_timestamps if now - ts < RATE_LIMIT_WINDOW])
    return {
        "limit": RATE_LIMIT_REQUESTS,
        "window_seconds": RATE_LIMIT_WINDOW,
        "current_requests": active_requests,
        "remaining": max(0, RATE_LIMIT_REQUESTS - active_requests)
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)
