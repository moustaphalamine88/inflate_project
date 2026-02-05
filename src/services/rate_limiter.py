"""
Task 10: Rate Limiter service.

Client-side rate limiter to enforce the external API's rate limit.

Requirements:
1. Respect a limit of 60 requests per minute.
2. When a 429 response is returned, wait for the `Retry-After` duration before retrying.
3. Enforce a global limit even when multiple tenants are ingesting concurrently.

Implementation:
- Sliding window (simple & effective for fixed rate limits).
- Uses asyncio.Lock for concurrency safety.
"""

import asyncio
import time
from typing import Optional
from collections import deque


class RateLimiter:
    """
    Sliding window rate limiter.

    Each successful acquire() consumes a "slot" by recording a timestamp.
    If the window is full, acquire() returns how long to wait.
    """

    def __init__(self, requests_per_minute: int = 60):
        self.requests_per_minute = requests_per_minute
        self.window_seconds = 60
        self.request_times: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> float:
        """
        Attempt to take a slot.

        Returns:
            0.0 if you can proceed immediately,
            otherwise the number of seconds to wait.
        """
        async with self._lock:
            now = time.time()

            # Drop timestamps outside the sliding window.
            while self.request_times and (now - self.request_times[0]) >= self.window_seconds:
                self.request_times.popleft()

            if len(self.request_times) < self.requests_per_minute:
                # Consume a slot now.
                self.request_times.append(now)
                return 0.0

            # Need to wait until the oldest request expires out of the window.
            wait_time = self.window_seconds - (now - self.request_times[0])
            return max(0.0, wait_time)

    async def wait_and_acquire(self) -> None:
        """
        Wait until a slot is available, then consume it.

        Note:
        - This must NOT double-consume.
        - It loops because the window can shift during sleep.
        """
        while True:
            wait_time = await self.acquire()
            if wait_time <= 0:
                return
            await asyncio.sleep(wait_time)

    def get_status(self) -> dict:
        """
        Return the current status of the rate limiter.

        Keep this method sync (tests call it without await).
        Best-effort cleanup without lock is acceptable for monitoring.
        """
        now = time.time()
        while self.request_times and (now - self.request_times[0]) >= self.window_seconds:
            self.request_times.popleft()

        current = len(self.request_times)
        return {
            "limit": self.requests_per_minute,
            "window_seconds": self.window_seconds,
            "current_requests": current,
            "remaining": max(0, self.requests_per_minute - current),
        }


# Global RateLimiter singleton instance (shared across all ingestions/tenants).
_global_rate_limiter: Optional[RateLimiter] = None


def get_rate_limiter() -> RateLimiter:
    """Return the global RateLimiter instance."""
    global _global_rate_limiter
    if _global_rate_limiter is None:
        _global_rate_limiter = RateLimiter(requests_per_minute=60)
    return _global_rate_limiter
