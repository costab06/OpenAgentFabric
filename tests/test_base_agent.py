
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from shared.agent import BaseAgent
from shared.eventbus.events import (
    DocumentDiscoveredEvent,
    DocumentDiscoveredPayload,
    DocumentFetchedEvent,
    DocumentFetchedPayload,
    EventType,
)
from shared.models import AgentName, ArtifactRef, ArtifactType


class MockFetcherAgent(BaseAgent):
    async def handle_event(self, event):
        return [
            DocumentFetchedEvent(
                source_agent=AgentName.FETCHER,
                correlation_id=event.correlation_id,
                causation_id=event.event_id,
                payload=DocumentFetchedPayload(
                    cik=event.payload.cik,
                    accession_number=event.payload.accession_number,
                    company_name=event.payload.company_name,
                    filing_type=event.payload.filing_type,
                    artifact=ArtifactRef(
                        artifact_id="test-filing-001",
                        artifact_type=ArtifactType.RAW_DOCUMENT,
                        uri="file:///tmp/test-filing.txt",
                        content_type="text/plain",
                    ),
                ),
            )
        ]


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


@pytest.fixture
def dependencies():
    bus = AsyncMock()
    registry = AsyncMock()

    registry.get.return_value = {"active": True}

    # New defaults for retry processing
    bus.get_attempt_count.return_value = 0
    bus.begin_attempt.return_value = 1
    bus.commit.return_value = True

    return bus, registry

@pytest.mark.asyncio
async def test_agent_processes_and_publishes(dependencies):
    bus, registry = dependencies

    agent = MockFetcherAgent(
        name=AgentName.FETCHER,
        event_bus=bus,
        correlation_registry=registry,
        subscribed_event_types={EventType.DOCUMENT_DISCOVERED},
    )

    event = make_event()

    await agent.process_message("123-0", event)

    bus.commit.assert_awaited_once()

    published = bus.commit.await_args.kwargs["downstream_events"][0]

    assert published.correlation_id == event.correlation_id
    assert published.causation_id == event.event_id
    assert published.source_agent == AgentName.FETCHER

    assert bus.commit.await_args.kwargs["source_event_id"] == event.event_id

    bus.publish.assert_not_awaited()
    bus.acknowledge.assert_not_awaited()

    registry.refresh.assert_awaited_once_with(event.correlation_id)


@pytest.mark.asyncio
async def test_agent_does_not_acknowledge_on_failure(dependencies):
    bus, registry = dependencies

    registry.get.return_value = None

    agent = MockFetcherAgent(
        name=AgentName.FETCHER,
        event_bus=bus,
        correlation_registry=registry,
        subscribed_event_types={EventType.DOCUMENT_DISCOVERED},
    )


    await agent.process_message("123-0", make_event())

    bus.commit.assert_not_awaited()
    bus.publish.assert_not_awaited()
    bus.acknowledge.assert_not_awaited()


@pytest.mark.asyncio
async def test_agent_ignores_unsubscribed_events(dependencies):
    bus, registry = dependencies

    agent = MockFetcherAgent(
        name=AgentName.FETCHER,
        event_bus=bus,
        correlation_registry=registry,
        subscribed_event_types={EventType.DOCUMENT_FETCHED},
    )

    await agent.process_message("123-0", make_event())

    bus.commit.assert_not_awaited()
    bus.publish.assert_not_awaited()
    bus.acknowledge.assert_awaited_once_with(
        agent.stream,
        agent.group,
        "123-0",
    )
