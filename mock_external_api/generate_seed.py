import json
import random
from datetime import datetime, timedelta

def generate_tickets(count=200):
    tenants = ["tenant_a", "tenant_b", "tenant_c", "tenant_d"]
    sources = ["email", "web", "chat", "api"]
    statuses = ["open", "closed", "pending"]
    keywords = [
        "refund", "charged twice", "lawsuit", "angry", "broken", "urgent", 
        "help", "question", "feature request", "login issue", "GDPR", "cancel"
    ]
    
    tickets = []
    start_date = datetime(2026, 1, 1)
    
    for i in range(count):
        ticket_id = f"ext-{i+1:03d}"
        tenant_id = random.choice(tenants)
        source = random.choice(sources)
        customer_id = f"cust_{random.randint(100, 999)}"
        subject = f"Issue {ticket_id}: {random.choice(keywords)}"
        message = f"This is a message about {random.choice(keywords)}. " * 3
        created_at = (start_date + timedelta(days=random.randint(0, 25), hours=random.randint(0, 23))).isoformat() + "Z"
        status = random.choice(statuses)
        
        tickets.append({
            "id": ticket_id,
            "tenant_id": tenant_id,
            "source": source,
            "customer_id": customer_id,
            "subject": subject,
            "message": message,
            "created_at": created_at,
            "status": status
        })
    
    # Sort by created_at
    tickets.sort(key=lambda x: x["created_at"])
    return tickets

if __name__ == "__main__":
    tickets = generate_tickets(10000)
    with open("mock_external_api/data/seed_tickets.json", "w") as f:
        json.dump(tickets, f, indent=2)
    print(f"Generated 10000 tickets in mock_external_api/data/seed_tickets.json")
