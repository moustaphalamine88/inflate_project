"""
Task 11: Circuit Breaker service.

State transition rules:
- CLOSED → OPEN: at least 5 failures in the last 10 requests.
- OPEN → HALF_OPEN: after 30 seconds.
- HALF_OPEN → CLOSED: 1 successful call.
- HALF_OPEN → OPEN: 1 failed call.

No external circuit breaker libraries allowed.
"""

import asyncio
import time
from enum import Enum
from typing import Callable, Any, Optional
from collections import deque
from dataclasses import dataclass


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    success_threshold: int = 1
    window_size: int = 10
    timeout_seconds: float = 30.0
    half_open_max_calls: int = 1


class CircuitBreakerOpenError(Exception):
    """Raised when a call is attempted while the circuit is OPEN."""
    def __init__(self, retry_after: float):
        self.retry_after = retry_after
        super().__init__(f"Circuit is OPEN. Retry after {retry_after:.1f} seconds")


class CircuitBreaker:
    """
    Circuit Breaker implementation (async-safe).

    Notes:
    - Uses a deque window to track last N results.
    - Uses a lock to protect state transitions under concurrency.
    """

    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()

        self._state = CircuitState.CLOSED
        self._opened_at: Optional[float] = None
        self._recent_results: deque[bool] = deque(maxlen=self.config.window_size)

        self._failure_count = 0
        self._success_count = 0

        self._lock = asyncio.Lock()
        self._half_open_calls = 0

    @property
    def state(self) -> CircuitState:
        """
        Return current state, applying automatic OPEN -> HALF_OPEN transition
        when timeout elapses.
        """
        if self._state == CircuitState.OPEN and self._opened_at is not None:
            if time.time() - self._opened_at >= self.config.timeout_seconds:
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
                self._success_count = 0
        return self._state

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Call an async function through the Circuit Breaker.

        - OPEN: fail fast (do not call func).
        - HALF_OPEN: allow limited concurrent probes.
        - CLOSED: normal behavior.
        """
        # Gatekeeping under lock (avoid executing func when OPEN).
        async with self._lock:
            current_state = self.state

            if current_state == CircuitState.OPEN:
                retry_after = 0.0
                if self._opened_at is not None:
                    remaining = self.config.timeout_seconds - (time.time() - self._opened_at)
                    retry_after = max(0.0, remaining)
                raise CircuitBreakerOpenError(retry_after)

            if current_state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.config.half_open_max_calls:
                    # Fail fast to prevent stampede in HALF_OPEN.
                    raise CircuitBreakerOpenError(0.0)
                self._half_open_calls += 1

        # Execute outside the lock.
        try:
            result = await func(*args, **kwargs)
        except Exception:
            async with self._lock:
                if self._state == CircuitState.HALF_OPEN:
                    self._half_open_calls = max(0, self._half_open_calls - 1)
                await self._on_failure()
            raise
        else:
            async with self._lock:
                if self._state == CircuitState.HALF_OPEN:
                    self._half_open_calls = max(0, self._half_open_calls - 1)
                await self._on_success()
            return result

    async def _on_success(self) -> None:
        """
        Record success:
        - HALF_OPEN -> CLOSED on 1 success (success_threshold=1 by spec).
        """
        self._recent_results.append(True)
        self._success_count += 1

        if self._state == CircuitState.HALF_OPEN:
            if self._success_count >= self.config.success_threshold:
                self._state = CircuitState.CLOSED
                self._opened_at = None
                self._failure_count = 0
                self._success_count = 0
                self._recent_results.clear()
                self._half_open_calls = 0

    async def _on_failure(self) -> None:
        """
        Record failure:
        - HALF_OPEN -> OPEN immediately on 1 failure.
        - CLOSED -> OPEN if failures in recent window >= threshold.
        """
        self._recent_results.append(False)
        self._failure_count += 1

        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._opened_at = time.time()
            self._half_open_calls = 0
            return

        if self._should_open():
            self._state = CircuitState.OPEN
            self._opened_at = time.time()
            self._half_open_calls = 0

    def _should_open(self) -> bool:
        """
        OPEN if at least failure_threshold failures in the last window_size calls.
        """
        failures = sum(1 for r in self._recent_results if not r)
        return failures >= self.config.failure_threshold

    def get_status(self) -> dict:
        """
        Return current status for API exposure.
        """
        current_state = self.state

        failure_rate = 0.0
        if self._recent_results:
            failures = sum(1 for r in self._recent_results if not r)
            failure_rate = failures / len(self._recent_results)

        retry_after = None
        if current_state == CircuitState.OPEN and self._opened_at is not None:
            remaining = self.config.timeout_seconds - (time.time() - self._opened_at)
            retry_after = max(0.0, remaining)

        return {
            "name": self.name,
            "state": current_state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "recent_failure_rate": failure_rate,
            "opened_at": self._opened_at,
            "retry_after": retry_after,
        }

    def reset(self) -> None:
        """Reset the circuit to a clean CLOSED state."""
        self._state = CircuitState.CLOSED
        self._opened_at = None
        self._recent_results.clear()
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0


_circuit_breakers: dict[str, CircuitBreaker] = {}


def get_circuit_breaker(name: str) -> CircuitBreaker:
    """Get or create a circuit breaker instance by name."""
    if name not in _circuit_breakers:
        _circuit_breakers[name] = CircuitBreaker(name)
    return _circuit_breakers[name]
