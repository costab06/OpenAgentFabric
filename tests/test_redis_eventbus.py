from uuid import uuid4

import pytest

from shared.eventbus.events import (
    DocumentDiscoveredEvent,
    DocumentDiscoveredPayload,
    EventType,
)
from shared.eventbus.redis import RedisEventBus
from shared.models import AgentName


@pytest.mark.asyncio
async def test_redis_eventbus_publish_subscribe_ack():
    bus = RedisEventBus(redis_url="redis://localhost:6380/0")

    # Unique stream and group prevent interference from previous test runs.
    unique_id = uuid4().hex
    stream = f"test.oaf.events.{unique_id}"
    group = f"test-group-{unique_id}"
    consumer = "test-consumer"

    event = DocumentDiscoveredEvent(
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

    try:
        await bus.publish(stream, event)

        received = False

        async for message_id, received_event in bus.subscribe(
            stream=stream,
            group=group,
            consumer=consumer,
            count=1,
            block_ms=1000,
        ):
            assert received_event.event_type == EventType.DOCUMENT_DISCOVERED
            assert received_event.payload.company_name == "Apple Inc."

            await bus.acknowledge(stream, group, message_id)
            received = True
            break

        assert received

    finally:
        await bus.redis.delete(stream)
        await bus.close()
        
