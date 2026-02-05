"""
Test Task 11: Circuit Breaker pattern.

These tests verify that the Circuit Breaker transitions state correctly
and protects the system in failure scenarios.
"""

import pytest
import asyncio
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_circuit_breaker_opens_on_failures():
    """
    After consecutive failures, the circuit should transition to the OPEN state.
    """
    from src.services.circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerConfig

    config = CircuitBreakerConfig(
        failure_threshold=3,
        window_size=5,
        timeout_seconds=1.0
    )
    cb = CircuitBreaker("test_service", config)

    # Function that always fails
    async def failing_func():
        raise Exception("Service unavailable")

    # Fail 3 times
    for i in range(3):
        try:
            await cb.call(failing_func)
        except Exception:
            pass

    # Circuit should now be in the OPEN state
    assert cb.state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_circuit_breaker_rejects_when_open():
    """
    While in the OPEN state, calls should fail fast without performing real work.
    """
    from src.services.circuit_breaker import (
        CircuitBreaker, CircuitState, CircuitBreakerConfig, CircuitBreakerOpenError
    )

    config = CircuitBreakerConfig(
        failure_threshold=2,
        window_size=5,
        timeout_seconds=10.0
    )
    cb = CircuitBreaker("test_open", config)

    # Manually force the circuit into the OPEN state
    cb._state = CircuitState.OPEN
    cb._opened_at = asyncio.get_event_loop().time()

    call_count = 0

    async def tracked_func():
        nonlocal call_count
        call_count += 1
        return "success"

    # Attempt to call while OPEN
    with pytest.raises(CircuitBreakerOpenError):
        await cb.call(tracked_func)

    # The underlying function must not be executed
    assert call_count == 0


@pytest.mark.asyncio
async def test_circuit_breaker_half_open_after_timeout():
    """
    After the timeout elapses in OPEN, the circuit should transition to HALF_OPEN.
    """
    from src.services.circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerConfig
    import time

    config = CircuitBreakerConfig(
        failure_threshold=2,
        window_size=5,
        timeout_seconds=0.1  # use a short timeout (100ms) for the test
    )
    cb = CircuitBreaker("test_half_open", config)

    # Force the circuit into OPEN
    cb._state = CircuitState.OPEN
    cb._opened_at = time.time() - 0.2  # opened 200ms ago

    # Accessing state should transition to HALF_OPEN automatically
    assert cb.state == CircuitState.HALF_OPEN


@pytest.mark.asyncio
async def test_circuit_breaker_closes_on_success():
    """
    A successful call in HALF_OPEN should transition the circuit to CLOSED.
    """
    from src.services.circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerConfig

    config = CircuitBreakerConfig(
        failure_threshold=2,
        window_size=5,
        timeout_seconds=0.1,
        success_threshold=1
    )
    cb = CircuitBreaker("test_close", config)

    # Force the circuit into HALF_OPEN
    cb._state = CircuitState.HALF_OPEN

    async def success_func():
        return "success"

    # Perform a successful call
    result = await cb.call(success_func)
    assert result == "success"

    # Circuit should now be CLOSED
    assert cb.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_circuit_status_api(client: AsyncClient):
    """
    Test the Circuit Breaker status API.
    """
    response = await client.get("/circuit/notify/status")
    assert response.status_code == 200

    data = response.json()
    assert "state" in data
    assert "failure_count" in data
    assert data["state"] in ["closed", "open", "half_open"]


@pytest.mark.asyncio
async def test_circuit_reset_api(client: AsyncClient):
    """
    Test the Circuit Breaker reset API.
    """
    response = await client.post("/circuit/notify/reset")
    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "reset"
