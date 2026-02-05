# src/services/http_client.py
from typing import Optional
import httpx

_client: Optional[httpx.AsyncClient] = None


async def init_http_client() -> None:
    """
    Create one AsyncClient for the whole app.

    This client is shared across requests (connection pooling).
    """
    global _client
    if _client is None:
        limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
        timeout = httpx.Timeout(connect=3.0, read=10.0, write=10.0, pool=10.0)
        _client = httpx.AsyncClient(limits=limits, timeout=timeout)


async def close_http_client() -> None:
    """Close the shared AsyncClient on shutdown."""
    global _client
    if _client is not None:
        await _client.aclose()
    _client = None


async def get_http_client() -> httpx.AsyncClient:
    """
    Lazy-safe getter.

    In tests, FastAPI startup hooks may not execute depending on the transport.
    To avoid crashes, we initialize the client on-demand.
    """
    if _client is None:
        await init_http_client()
    return _client
