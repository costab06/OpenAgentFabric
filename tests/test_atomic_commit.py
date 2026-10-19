from uuid import uuid4

import pytest

from shared.eventbus.events import (
    DocumentDiscoveredEvent,
    DocumentDiscoveredPayload,
    DocumentFetchedEvent,
    DocumentFetchedPayload,
)
from shared.eventbus.redis import RedisEventBus
from shared.models import AgentName, ArtifactRef, ArtifactType


def make_discovered_event():
    return DocumentDiscoveredEvent(
        source_agent=AgentName.WATCHER,
        payload=DocumentDiscoveredPayload(
            cik="0000320193",
            accession_number="0000320193-24-000123",
            company_name="Apple Inc.",
            filing_type="10-Q",
            filing_date="2024-08-02",
            filing_url="https://www.sec.gov/example",
        ),
    )


def make_fetched_event(source):
    return DocumentFetchedEvent(
        source_agent=AgentName.FETCHER,
        correlation_id=source.correlation_id,
        causation_id=source.event_id,
        payload=DocumentFetchedPayload(
            cik=source.payload.cik,
            accession_number=source.payload.accession_number,
            company_name=source.payload.company_name,
            filing_type=source.payload.filing_type,
            artifact=ArtifactRef(
                artifact_id="test-artifact-001",
                artifact_type=ArtifactType.RAW_DOCUMENT,
                uri="file:///tmp/test-filing.txt",
                content_type="text/plain",
            ),
        ),
    )


@pytest.mark.asyncio
async def test_atomic_commit_publishes_and_acknowledges():
    bus = RedisEventBus("redis://localhost:6380/0")

    unique = uuid4().hex
    stream = f"test.oaf.atomic.{unique}"
    group = f"test.atomic.{unique}"

    source = make_discovered_event()
    downstream = make_fetched_event(source)

    terminal_key = f"oaf:terminal:{stream}:{group}:{source.event_id}"

    try:
        await bus.publish(stream, source)

        async for message_id, received in bus.subscribe(
            stream=stream,
            group=group,
            consumer="consumer-1",
            count=1,
            block_ms=1000,
        ):
            assert received.event_id == source.event_id
            break

        committed = await bus.commit(
            stream=stream,
            group=group,
            message_id=message_id,
            source_event_id=source.event_id,
            downstream_events=[downstream],
        )

        assert committed is True

        messages = await bus.redis.xrange(stream)
        assert len(messages) == 2

        pending = await bus.redis.xpending(stream, group)
        assert pending["pending"] == 0

        assert await bus.redis.exists(terminal_key)

    finally:
        await bus.redis.delete(stream, terminal_key)
        await bus.close()


@pytest.mark.asyncio
async def test_atomic_commit_does_not_duplicate_events():
    bus = RedisEventBus("redis://localhost:6380/0")

    unique = uuid4().hex
    stream = f"test.oaf.atomic.{unique}"
    group = f"test.atomic.{unique}"

    source = make_discovered_event()
    downstream = make_fetched_event(source)

    completion_key = f"oaf:committed:{stream}:{group}:{source.event_id}"

    try:
        await bus.publish(stream, source)

        async for message_id, _ in bus.subscribe(
            stream=stream,
            group=group,
            consumer="consumer-1",
            count=1,
            block_ms=1000,
        ):
            break

        first = await bus.commit(
            stream, group, message_id, source.event_id, [downstream]
        )

        second = await bus.commit(
            stream, group, message_id, source.event_id, [downstream]
        )

        assert first is True
        assert second is False

        messages = await bus.redis.xrange(stream)
        assert len(messages) == 2

    finally:
        await bus.redis.delete(stream, completion_key)
        await bus.close()

        
