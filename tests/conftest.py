# tests/conftest.py
import asyncio
import pytest
import pytest_asyncio
import httpx

from src.main import app
from src.db.mongo import init_mongo, close_mongo
from src.services.http_client import init_http_client, close_http_client


@pytest.fixture(scope="session")
def event_loop():
    """
    One event loop for the entire test session.
    Prevents 'Event loop is closed' with Motor + strict asyncio mode.
    """
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session", autouse=True)
def _init_global_resources(event_loop):
    """
    Initialize global singletons once per test session.
    Keeps Mongo and shared HTTP client alive for background tasks.
    """
    event_loop.run_until_complete(init_mongo())
    event_loop.run_until_complete(init_http_client())
    yield
    event_loop.run_until_complete(close_http_client())
    event_loop.run_until_complete(close_mongo())


@pytest_asyncio.fixture
async def client():
    """
    Async client required by async tests (await client.post()).
    """
    # ASGI transport to call FastAPI app in-process
    transport_kwargs = {"app": app}
    if "lifespan" in httpx.ASGITransport.__init__.__code__.co_varnames:
        transport_kwargs["lifespan"] = "on"

    transport = httpx.ASGITransport(**transport_kwargs)

    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
