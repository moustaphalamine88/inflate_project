from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

class TicketBase(BaseModel):
    external_id: str = Field(..., alias="id")
    tenant_id: str
    source: str
    customer_id: str
    subject: str
    message: str
    created_at: datetime
    status: str

class TicketInDB(TicketBase):
    id: Optional[str] = Field(None, alias="_id")
    urgency: str
    sentiment: str
    requires_action: bool

class TicketResponse(TicketInDB):
    pass

class TicketHistory(BaseModel):
    """Ticket change history model (Task 12)."""
    ticket_id: str
    tenant_id: str
    action: str  # created, updated, deleted
    changes: Optional[dict] = None
    recorded_at: datetime


class TenantStats(BaseModel):
    total_tickets: int
    by_status: dict
    urgency_high_ratio: float
    negative_sentiment_ratio: float
    hourly_trend: Optional[List[dict]] = None
    top_keywords: Optional[List[str]] = None
    at_risk_customers: Optional[List[dict]] = None
