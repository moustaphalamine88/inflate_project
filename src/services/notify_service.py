"""
NotifyService (Task 4 + Task 11)

Goals:
- Deliver notifications for high-urgency tickets to an unstable external endpoint.
- Manual retries only (no tenacity/backoff).
- Circuit Breaker compliant with the project spec.
- Should not crash ingestion even if notify endpoint is down.

This service is called by IngestService via async workers / queue.
"""

import asyncio
import random
from typing import Optional, Dict, Any

import httpx

from src.core.logging import logger
from src.services.http_client import get_http_client
from src.services.circuit_breaker import get_circuit_breaker, CircuitBreakerOpenError


class NotifyService:
    def __init__(self):
        # External unstable endpoint
        self.notify_url = "http://mock-external-api:9000/notify"

        # Manual retry policy
        self.max_attempts = 5
        self.base_delay = 0.2  # seconds

        # Circuit breaker name (used by /circuit/notify/status)
        self.cb_name = "notify"

    async def notify_high_priority(
        self,
        tenant_id: str,
        ticket_id: str,
        classification: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Send a high-priority notification.

        Raises:
            CircuitBreakerOpenError if circuit is OPEN (fail fast).
            RuntimeError if retries exhausted.
        """
        cb = get_circuit_breaker(self.cb_name)
        client = await get_http_client()

        payload = {
            "tenant_id": tenant_id,
            "ticket_id": ticket_id,
            "urgency": classification.get("urgency", "high"),
            "sentiment": classification.get("sentiment"),
            "requires_action": classification.get("requires_action", True),
            "reason": classification.get("reason", "high_priority"),
        }

        async def _do_call() -> Dict[str, Any]:
            resp = await client.post(self.notify_url, json=payload)
            resp.raise_for_status()
            return resp.json()

        last_err: Optional[Exception] = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                # Circuit breaker wraps the real HTTP call.
                return await cb.call(_do_call)

            except CircuitBreakerOpenError:
                # Fail fast when OPEN (spec requirement).
                raise

            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as e:
                # Transient errors: retry with exponential backoff + jitter.
                last_err = e
                delay = self.base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
                logger.warning(
                    f"[notify] attempt {attempt}/{self.max_attempts} failed "
                    f"tenant={tenant_id} ticket={ticket_id} err={e} -> sleep {delay:.2f}s"
                )
                await asyncio.sleep(delay)

            except Exception as e:
                # Unknown errors: still retry a bit.
                last_err = e
                delay = self.base_delay + random.uniform(0, 0.1)
                logger.warning(
                    f"[notify] unexpected error attempt {attempt}/{self.max_attempts} "
                    f"tenant={tenant_id} ticket={ticket_id} err={e} -> sleep {delay:.2f}s"
                )
                await asyncio.sleep(delay)

        raise RuntimeError(f"Notification failed after {self.max_attempts} attempts: {last_err}")
