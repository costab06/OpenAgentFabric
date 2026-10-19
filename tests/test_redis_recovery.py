from uuid import uuid4

import pytest

from shared.eventbus.events import (
    DocumentDiscoveredEvent,
    DocumentDiscoveredPayload,
)
from shared.eventbus.redis import RedisEventBus
from shared.models import AgentName


@pytest.mark.asyncio
async def test_abandoned_message_can_be_reclaimed():
    bus = RedisEventBus("redis://localhost:6380/0")

    unique = uuid4().hex
    stream = f"test.oaf.recovery.{unique}"
    group = f"test.recovery.{unique}"

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

        # Consumer 1 receives the event but crashes without acknowledging.
        async for message_id, received in bus.subscribe(
            stream=stream,
            group=group,
            consumer="consumer-1",
            count=1,
            block_ms=1000,
        ):
            assert received.event_id == event.event_id
            break

        # Consumer 2 reclaims the abandoned message.
        reclaimed = await bus.reclaim_pending(
            stream=stream,
            group=group,
            consumer="consumer-2",
            min_idle_ms=0,
        )

        assert len(reclaimed) == 1

        reclaimed_id, reclaimed_event = reclaimed[0]

        assert reclaimed_id == message_id
        assert reclaimed_event.event_id == event.event_id

        await bus.acknowledge(stream, group, reclaimed_id)

        # No unacknowledged messages should remain.
        pending = await bus.redis.xpending(stream, group)
        assert pending["pending"] == 0

    finally:
        await bus.redis.delete(stream)
        await bus.close()
        
