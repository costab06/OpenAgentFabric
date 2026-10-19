
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from shared.agent.base import BaseAgent
from shared.correlation.registry import CorrelationRegistry
from shared.eventbus.events import (
    DocumentDiscoveredEvent,
    DocumentDiscoveredPayload,
    EventType,
)
from shared.eventbus.redis import RedisEventBus
from shared.models import AgentName


class Agent(BaseAgent):
    def __init__(self, bus, registry, stream="oaf.test", **kwargs):
        super().__init__(
            name=AgentName.FETCHER,
            event_bus=bus,
            correlation_registry=registry,
            subscribed_event_types={EventType.DOCUMENT_DISCOVERED},
            stream=stream,
            **kwargs,
        )
        self.calls = 0

    async def handle_event(self, event):
        self.calls += 1
        return []


def mock_agent():
    return Agent(AsyncMock(), AsyncMock())


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["committed", "dead_lettered"])
async def test_lost_response_reads_terminal_state(state):
    agent = mock_agent()
    agent.event_bus.get_terminal_state.return_value = state

    async def lost_response():
        raise ConnectionError("response lost")

    result = await agent._reconcile_terminal_operation(
        lost_response(), uuid4(), "committed"
    )
    assert result == state
    agent.event_bus.get_terminal_state.assert_awaited_once()


@pytest.mark.asyncio
async def test_absent_terminal_state_propagates_original_error():
    agent = mock_agent()
    agent.event_bus.get_terminal_state.return_value = None

    async def lost_response():
        raise ConnectionError("response lost")

    with pytest.raises(ConnectionError, match="response lost"):
        await agent._reconcile_terminal_operation(
            lost_response(), uuid4(), "committed"
        )


@pytest.mark.asyncio
async def test_failed_lookup_propagates_infrastructure_error():
    agent = mock_agent()
    agent.event_bus.get_terminal_state.side_effect = ConnectionError(
        "lookup unavailable"
    )

    async def lost_response():
        raise ConnectionError("response lost")

    with pytest.raises(ConnectionError, match="lookup unavailable"):
        await agent._reconcile_terminal_operation(
            lost_response(), uuid4(), "committed"
        )


@pytest.mark.asyncio
async def test_commit_succeeds_but_response_lost():
    bus = RedisEventBus("redis://localhost:6380/0")
    registry = CorrelationRegistry(bus.redis)
    stream = f"oaf.test.reconcile.{uuid4().hex}"
    agent = Agent(bus, registry, stream, consumer="consumer-a")
    event = None
    message_id = None

    try:
        record = await registry.create(
            trigger_type="terminal-reconciliation-test",
            trigger_timestamp=datetime.now(timezone.utc),
            business_fields={"stream": stream},
        )
        event = DocumentDiscoveredEvent(
            source_agent=AgentName.WATCHER,
            correlation_id=UUID(str(record.correlation_id)),
            payload=DocumentDiscoveredPayload(
                cik="0000320193",
                accession_number="0000320193-24-000123",
                company_name="Apple Inc.",
                filing_type="10-Q",
                filing_date="2024-08-02",
                filing_url="https://www.sec.gov/example",
            ),
        )
        message_id = await bus.publish(stream, event)
        async for received_id, received_event in bus.subscribe(
            stream=stream,
            group=agent.group,
            consumer=agent.consumer,
            block_ms=100,
        ):
            assert received_id == message_id
            event = received_event
            break

        actual_commit = bus.commit
        calls = 0

        async def commit_then_lose_response(**kwargs):
            nonlocal calls
            calls += 1
            await actual_commit(**kwargs)
            raise ConnectionError("simulated lost commit response")

        bus.commit = commit_then_lose_response

        await agent.process_message(message_id, event)
        assert agent.calls == 1
        assert calls == 1
        assert await bus.get_terminal_state(
            stream, agent.group, event.event_id
        ) == "committed"
        assert await bus.get_pending_message(
            stream, agent.group, message_id
        ) is None
        assert await bus.redis.xlen(stream) == 1

    finally:
        if event is not None:
            await bus.redis.delete(
                f"oaf:terminal:{stream}:{agent.group}:{event.event_id}"
            )
            await bus.redis.delete(
                f"oaf:attempts:{stream}:{agent.group}:{message_id}"
            )
        await bus.redis.delete(stream)
        await bus.close()
