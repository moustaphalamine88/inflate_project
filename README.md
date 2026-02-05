# Multi-tenant SaaS Support Ticket Analysis System

## Problem Statement
Our company provides a support ticket management platform for multiple tenants. We need a production-ready backend system that can:
1. **Ingest** tickets from an external SaaS provider with pagination and idempotency.
2. **Classify** tickets based on urgency, sentiment, and actionability.
3. **Store** tickets efficiently in MongoDB with optimized indexing.
4. **Analyze** ticket data using high-performance database queries.
5. **Monitor** system health and dependencies.
6. **Audit** all ingestion processes for traceability.

---

## ⚠️ CRITICAL CONSTRAINTS (Spec)
To keep the challenge realistic and comparable across candidates, the following constraints are **MANDATORY**. These are the only hard rules we will use for pass/fail:

1.  **No External Retry Libraries**  
    Do NOT use `tenacity`, `backoff`, or any other retry library. Implement retry behaviour yourself using Python's standard `asyncio` primitives.

2.  **Database‑Centric Analytics**  
    The `/stats` endpoint must compute its metrics **inside MongoDB** (or within the database layer), not in Python loops over large result sets.
    - It should comfortably handle **10,000+ tickets** without breaching a roughly **2‑second** response budget in the provided `docker-compose` setup.
    - Solutions that repeatedly load full collections into memory or iterate over every document in Python will be treated as failing this requirement, even if they "work" on small data.

3.  **Manual Pagination**  
    You are expected to control pagination from your own code (looping over pages, handling termination, etc.). Avoid high‑level client helpers that hide pagination logic.

4.  **No External Distributed Lock Libraries**  
    Do NOT use libraries such as `redis-lock`, `pottery`, or `sherlock`. Implement your own lock mechanism using MongoDB's atomic operations (for example, `findOneAndUpdate`).

---

## Requirements (All Tasks are Mandatory)

### Task 1: Reliable Data Ingestion
- Implement `POST /ingest/run?tenant_id=...` to fetch tickets from the Mock External API.
- Ensure the system fetches all available data from the paginated external source.
- The ingestion process must be idempotent. Re-running ingestion for the same tenant should not result in duplicate records.

### Task 2: Classification Logic
Implement rule-based classification in `ClassifyService`:
- Determine **Urgency** (high/medium/low), **Sentiment** (negative/neutral/positive), and **Requires Action** (boolean).
- Use keywords such as "refund", "lawsuit", "urgent", "angry", "broken", "GDPR" to determine these values.

### Task 3: Advanced Analytics & Reporting
Design `GET /tenants/{tenant_id}/stats` so that it can be used to power a tenant‑level analytics dashboard.
- At a minimum, include: `total_tickets`, `by_status` (counts), `urgency_high_ratio`, and an `hourly_trend` of ticket creation over the last 24 hours.
- You should rely on MongoDB's aggregation capabilities (or equivalent DB‑side operators) rather than ad‑hoc Python post‑processing.
- Optionally, you may add richer analytics such as `top_keywords` or `at_risk_customers`.

### Task 4: Reliable Alerting & Retries
- When a ticket is classified as high priority, call the notification service: `POST http://mock-external-api:9000/notify`.
- The notification service is intentionally unstable (it may return 5xx responses or be slow). You are responsible for ensuring that high-priority notifications are successfully delivered without blocking the main ingestion flow.
- Implement a robust handling strategy that accounts for transient failures and maintains system availability.

### Task 5: System Health Monitoring
- Implement `GET /health` to report the status of the system and its critical dependencies. Return a non-200 status if any dependency is unavailable.

### Task 6: Ingestion Audit Logging
- Record the history of every ingestion run in an `ingestion_logs` collection.
- Logs must include timestamps, final status, and processing metrics. Traceability must be maintained even if the process fails.

### Task 7: Resource Management & Stability
- Ensure that your database connection handling and resource usage remain stable under sustained load (e.g., during high-volume ingestion and simultaneous analytics requests).
- The system should exhibit production-ready behavior, avoiding common pitfalls related to connection lifecycle and resource exhaustion.

### Task 8: Concurrent Ingestion Control
- Prevent concurrent `POST /ingest/run` executions for the same tenant.
- If an ingestion job is already running for a tenant, return **409 Conflict**.
- Implement a lock mechanism using MongoDB atomic operations (e.g., `findOneAndUpdate`), and ensure locks expire automatically if they are not refreshed within approximately **60 seconds**.
- Provide an ingestion status endpoint: `GET /ingest/status?tenant_id=...`.

### Task 9: Ingestion Job Management
- When ingestion starts, generate a `job_id` and include it in the response.
- Implement `GET /ingest/progress/{job_id}` to report job status and progress (e.g., `{"job_id": "...", "status": "running", "progress": 45, "total_pages": 100, "processed_pages": 45}`).
- Implement `DELETE /ingest/{job_id}` to allow graceful cancellation of a running job. Preserve already-ingested data and record the final job status (e.g., `cancelled`).

### Task 10: External Rate Limiting
- The Mock External API is limited to **60 calls per minute** and will return `429 Too Many Requests` with a `Retry-After` header if the limit is exceeded.
- Implement global rate limiting so that total outbound calls stay within this limit, even when multiple tenants are ingesting in parallel.
- On `429` responses, wait for the `Retry-After` duration before retrying.
- You may implement any algorithm (e.g., token bucket, sliding window) and may use external **rate limiting** libraries.

### Task 11: Circuit Breaker for Notifications
- Implement a Circuit Breaker for the notification endpoint `POST http://mock-external-api:9000/notify`.
- Apply the following state transitions:
  - CLOSED → OPEN: at least 5 failures in the last 10 requests.
  - OPEN → HALF_OPEN: after approximately 30 seconds.
  - HALF_OPEN → CLOSED: 1 successful request.
  - HALF_OPEN → OPEN: 1 failed request.
- While in the OPEN state, fail fast without performing real HTTP calls.
- Expose the current circuit state via `GET /circuit/notify/status`.
- Do not use external Circuit Breaker libraries (e.g., `pybreaker`).

### Task 12: Change Detection & Synchronization
- Use the external ticket `updated_at` field to only update tickets that have changed.
- When tickets are deleted externally, apply a soft delete by setting `deleted_at` and exclude them from normal queries.
- Record field-level change history in a `ticket_history` collection so that ticket updates can be audited over time.

### Debug Task A: Multi-tenant Isolation
- The ticket listing API must never leak data across tenants.
- Review your `/tickets` implementation and make sure that results are always scoped to the requested `tenant_id`, even when additional filters (status, urgency, source, pagination) are applied.

### Debug Task B: Classification Quality
- The provided rule-based classification is intentionally simplistic and contains edge cases.
- Review and refine `ClassifyService` so that:
  - obviously critical tickets (e.g., strong refund / chargeback / legal threat signals) are treated with appropriate urgency,
  - sentiment and `requires_action` remain consistent with your rule set.

### Debug Task C: Memory Leak
- Repeated ingestion runs (e.g., 100+ times) cause memory usage to grow over time.
- Identify and fix the memory leak (for example, module-level caches or collections that are never cleaned up).

### Debug Task D: Race Condition
- Under concurrent `POST /ingest/run` calls for the same tenant, ingestion can sometimes run twice instead of being rejected.
- Find the check-then-act race condition and fix it using an atomic operation (for example, a single MongoDB atomic update).

### Debug Task E: Slow Stats Query
- When there are 10,000+ tickets, `GET /tenants/{tenant_id}/stats` can take more than 5 seconds.
- Use `explain()` to analyze the query plan and optimize indexes and/or the aggregation pipeline.
- Target a response time of roughly **≤ 500ms** for 10,000 tickets in the provided `docker-compose` environment.

## Getting Started
1. `cp .env.example .env`
2. `docker-compose up --build`
3. Run tests: `docker-compose exec app pytest`

---

## Follow-up Discussion (Interview)
During the live interview, we may ask you to:
- **Architecture & Design**: Discuss alternative designs for tenants with very different traffic patterns (e.g., massive spikes vs. steady flow).
- **Code Quality**: Review and critique a small piece of ingestion-related code provided during the session.
- **Resiliency**: Reason about failure and recovery scenarios around notification delivery and state consistency.
- **Operational Insight**: Analyze system logs to identify performance bottlenecks or production incidents.
