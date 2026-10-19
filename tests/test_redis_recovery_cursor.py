
from uuid import uuid4

import pytest

from shared.eventbus.events import (
    DocumentDiscoveredEvent,
    DocumentDiscoveredPayload,
)
from shared.eventbus.redis import RedisEventBus
from shared.models import AgentName


REDIS_URL = "redis://localhost:6380/0"


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


@pytest.mark.asyncio
async def test_cursor_based_recovery():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.cursor.{uuid4().hex}"
    group = "cursor-test-group"

    try:
        events = [make_event() for _ in range(5)]

        for event in events:
            await bus.publish(stream, event)

        # Deliver all five messages to a consumer without acknowledging.
        delivered = []

        async for message_id, event in bus.subscribe(
            stream=stream,
            group=group,
            consumer="crashed-consumer",
            count=5,
            block_ms=100,
        ):
            delivered.append(message_id)

            if len(delivered) == 5:
                break

        assert len(delivered) == 5

        # Recover the backlog in small batches.
        cursor = "0-0"
        recovered = []

        for _ in range(20):
            cursor, batch = await bus.reclaim_pending_batch(
                stream=stream,
                group=group,
                consumer="recovery-consumer",
                cursor=cursor,
                min_idle_ms=0,
                count=2,
            )

            recovered.extend(batch)

            if cursor == "0-0":
                break

        recovered_ids = [message_id for message_id, _ in recovered]

        assert len(recovered_ids) == 5
        assert set(recovered_ids) == set(delivered)
        assert len(set(recovered_ids)) == 5
        assert cursor == "0-0"

    finally:
        await bus.redis.delete(stream)
        await bus.close()
