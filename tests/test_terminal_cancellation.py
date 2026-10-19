
import asyncio
from unittest.mock import AsyncMock

import pytest

from shared.agent.base import BaseAgent
from shared.models import AgentName


class TestAgent(BaseAgent):
    async def handle_event(self, event):
        return []


def make_agent():
    bus = AsyncMock()
    registry = AsyncMock()

    return TestAgent(
        name=AgentName.FETCHER,
        event_bus=bus,
        correlation_registry=registry,
        subscribed_event_types=set(),
    )


@pytest.mark.asyncio
async def test_terminal_operation_completes_after_cancellation():
    agent = make_agent()

    started = asyncio.Event()
    release = asyncio.Event()
    completed = asyncio.Event()

    async def slow_commit():
        started.set()
        await release.wait()
        completed.set()
        return True

    processing = asyncio.create_task(
        agent._run_terminal_operation(slow_commit())
    )

    await asyncio.wait_for(started.wait(), timeout=2)

    processing.cancel()

    # Allow the cancellation to reach the helper.
    await asyncio.sleep(0)

    assert not completed.is_set()

    release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(processing, timeout=2)

    assert completed.is_set()


@pytest.mark.asyncio
async def test_terminal_operation_returns_success():
    agent = make_agent()

    async def successful_commit():
        return True

    result = await agent._run_terminal_operation(
        successful_commit()
    )

    assert result is True


@pytest.mark.asyncio
async def test_terminal_operation_propagates_redis_error():
    agent = make_agent()

    async def failed_commit():
        raise RuntimeError("Simulated Redis failure")

    with pytest.raises(
        RuntimeError,
        match="Simulated Redis failure",
    ):
        await agent._run_terminal_operation(
            failed_commit()
        )
