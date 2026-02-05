import pytest
import asyncio
from src.services.notify_service import NotifyService

@pytest.mark.asyncio
async def test_notify_retry_logic():
    # TODO: Mock the notification endpoint to return 500s then 200
    # TODO: Verify that NotifyService retries the call
    # TODO: Verify exponential backoff is applied
    pass
