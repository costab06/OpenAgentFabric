
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from shared.agent.base import BaseAgent
from shared.eventbus.events import (
    DocumentDiscoveredEvent,
    DocumentDiscoveredPayload,
    EventType,
)
from shared.models import AgentName


def make_event():
    return DocumentDiscoveredEvent(
        source_agent=AgentName.WATCHER,
        correlation_id=uuid4(),
        payload=DocumentDiscoveredPayload(
            cik="0000320193",
            accession_number="0000320193-24-000123",
            company_name="Apple Inc.",
            filing_type="10-Q",
            filing_date="2024-08-02",
            filing_url="https://www.sec.gov/example",
        ),
    )


class RetryAgent(BaseAgent):
    def __init__(self, bus, registry, failures=0):
        super().__init__(
            name=AgentName.FETCHER,
            event_bus=bus,
            correlation_registry=registry,
            subscribed_event_types={
                EventType.DOCUMENT_DISCOVERED
            },
            consumer="retry-test-consumer",
            max_attempts=5,
        )

        self.failures_remaining = failures
        self.handler_calls = 0

    async def handle_event(self, event):
        self.handler_calls += 1

        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise ValueError("Simulated handler failure")

        return []


@pytest.fixture
def dependencies():
    bus = AsyncMock()
    registry = AsyncMock()

    bus.get_attempt_count.return_value = 0
    bus.begin_attempt.return_value = 1
    bus.commit.return_value = True
    bus.dead_letter.return_value = True
    registry.get.return_value = {"active": True}

    return bus, registry


@pytest.mark.asyncio
async def test_first_failure_remains_pending(dependencies):
    bus, registry = dependencies
    agent = RetryAgent(bus, registry, failures=1)
    event = make_event()

    await agent.process_message("1-0", event)

    assert agent.handler_calls == 1
    bus.commit.assert_not_awaited()
    bus.dead_letter.assert_not_awaited()
    bus.acknowledge.assert_not_awaited()


@pytest.mark.asyncio
async def test_successful_retry_commits(dependencies):
    bus, registry = dependencies
    agent = RetryAgent(bus, registry, failures=1)
    event = make_event()

    bus.begin_attempt.side_effect = [1, 2]

    await agent.process_message("1-0", event)
    await agent.process_message("1-0", event)

    assert agent.handler_calls == 2
    bus.commit.assert_awaited_once()
    bus.dead_letter.assert_not_awaited()


@pytest.mark.asyncio
async def test_fifth_failure_dead_letters(dependencies):
    bus, registry = dependencies
    agent = RetryAgent(bus, registry, failures=5)
    event = make_event()

    bus.begin_attempt.return_value = 5

    await agent.process_message("1-0", event)

    assert agent.handler_calls == 1
    bus.dead_letter.assert_awaited_once()

    kwargs = bus.dead_letter.await_args.kwargs

    assert kwargs["attempts"] == 5
    assert kwargs["consumer"] == agent.consumer
    assert kwargs["source_event_id"] == event.event_id

    bus.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_fifth_attempt_can_succeed(dependencies):
    bus, registry = dependencies
    agent = RetryAgent(bus, registry, failures=0)
    event = make_event()

    bus.get_attempt_count.return_value = 4
    bus.begin_attempt.return_value = 5

    await agent.process_message("1-0", event)

    assert agent.handler_calls == 1
    bus.commit.assert_awaited_once()
    bus.dead_letter.assert_not_awaited()


@pytest.mark.asyncio
async def test_exhausted_interrupted_attempt_dead_letters(
    dependencies,
):
    bus, registry = dependencies
    agent = RetryAgent(bus, registry)
    event = make_event()

    bus.get_attempt_count.return_value = 5

    await agent.process_message("1-0", event)

    assert agent.handler_calls == 0
    bus.begin_attempt.assert_not_awaited()
    bus.dead_letter.assert_awaited_once()
    bus.commit.assert_not_awaited()
