
import asyncio
from unittest.mock import AsyncMock

import pytest

from shared.eventbus.events import EventType
from shared.models import AgentName

from test_base_agent import MockFetcherAgent, make_event


def make_agent(
    recovery_interval=0.01,
    min_idle_ms=30_000,
):
    bus = AsyncMock()

    bus.get_attempt_count.return_value = 0
    bus.begin_attempt.return_value = 1
    bus.commit.return_value = True


    registry = AsyncMock()

    registry.get.return_value = object()
    registry.refresh.return_value = True

    agent = MockFetcherAgent(
        name=AgentName.FETCHER,
        event_bus=bus,
        correlation_registry=registry,
        subscribed_event_types={EventType.DOCUMENT_DISCOVERED},
        recovery_interval=recovery_interval,
        min_idle_ms=min_idle_ms,
    )

    return agent, bus


@pytest.mark.asyncio
async def test_periodic_recovery_while_running():
    agent, bus = make_agent()

    event = make_event()
    calls = 0

    async def reclaim(**kwargs):
        nonlocal calls
        calls += 1

        if calls == 2:
            return "0-0", [("123-0", event)]

        return "0-0", []

    bus.reclaim_pending_batch.side_effect = reclaim

    # Keep the subscription alive without delivering messages.
    async def subscribe(**kwargs):
        while True:
            await asyncio.sleep(1)
            if False:
                yield None

    bus.subscribe = subscribe

    task = asyncio.create_task(agent.run())

    try:
        await asyncio.wait_for(
            wait_until(lambda: bus.commit.await_count == 1),
            timeout=1,
        )

        assert calls >= 2
        bus.commit.assert_awaited_once()

    finally:
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_recovery_respects_minimum_idle_time():
    agent, bus = make_agent(min_idle_ms=45_000)

    bus.reclaim_pending_batch.return_value = ("0-0", [])

    agent._running = True

    await agent._recover_pending_once()

    kwargs = bus.reclaim_pending_batch.await_args.kwargs

    assert kwargs["min_idle_ms"] == 45_000
    assert kwargs["count"] == agent.recovery_batch_size


@pytest.mark.asyncio
async def test_recovery_continues_after_message_failure():
    agent, bus = make_agent()

    first = make_event()
    second = make_event()

    agent._running = True

    bus.reclaim_pending_batch.return_value = (
        "0-0",
        [
            ("100-0", first),
            ("101-0", second),
        ],
    )

    original_handler = agent.handle_event

    async def handler(event):
        if event.event_id == first.event_id:
            raise RuntimeError("Simulated processing failure")

        return await original_handler(event)

    agent.handle_event = handler

    await agent._recover_pending_once()

    bus.commit.assert_awaited_once()

    committed_id = (
        bus.commit.await_args.kwargs["source_event_id"]
    )

    assert committed_id == second.event_id


@pytest.mark.asyncio
async def test_recovery_task_stops_on_cancellation():
    agent, bus = make_agent()

    bus.reclaim_pending_batch.return_value = ("0-0", [])

    async def subscribe(**kwargs):
        while True:
            await asyncio.sleep(1)
            if False:
                yield None

    bus.subscribe = subscribe

    task = asyncio.create_task(agent.run())

    await asyncio.sleep(0.03)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert agent._running is False


async def wait_until(predicate):
    while not predicate():
        await asyncio.sleep(0.001)
