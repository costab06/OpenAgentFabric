
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


async def prepare_pending(bus, stream, group, event):
    message_id = await bus.publish(stream, event)

    async for received_id, _ in bus.subscribe(
        stream=stream,
        group=group,
        consumer="consumer-a",
        block_ms=100,
    ):
        assert received_id == message_id
        return message_id

    raise AssertionError("No message received")


@pytest.mark.asyncio
async def test_pending_message_reports_owner_and_delivery_count():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.pending.{uuid4().hex}"
    group = "pending-test-group"

    try:
        event = make_event()
        message_id = await prepare_pending(
            bus, stream, group, event
        )

        pending = await bus.get_pending_message(
            stream, group, message_id
        )

        assert pending is not None
        assert pending.message_id == message_id
        assert pending.consumer == "consumer-a"
        assert pending.delivery_count == 1
        assert pending.idle_ms >= 0

    finally:
        await bus.redis.delete(stream)
        await bus.close()


@pytest.mark.asyncio
async def test_reclaim_updates_owner_and_delivery_count():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.pending.{uuid4().hex}"
    group = "pending-test-group"

    try:
        event = make_event()
        message_id = await prepare_pending(
            bus, stream, group, event
        )

        _, recovered = await bus.reclaim_pending_batch(
            stream=stream,
            group=group,
            consumer="consumer-b",
            min_idle_ms=0,
        )

        assert len(recovered) == 1
        assert recovered[0][0] == message_id

        pending = await bus.get_pending_message(
            stream, group, message_id
        )

        assert pending is not None
        assert pending.consumer == "consumer-b"
        assert pending.delivery_count == 2

    finally:
        await bus.redis.delete(stream)
        await bus.close()


@pytest.mark.asyncio
async def test_acknowledged_message_is_not_pending():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.pending.{uuid4().hex}"
    group = "pending-test-group"

    try:
        event = make_event()
        message_id = await prepare_pending(
            bus, stream, group, event
        )

        await bus.acknowledge(stream, group, message_id)

        pending = await bus.get_pending_message(
            stream, group, message_id
        )

        assert pending is None

    finally:
        await bus.redis.delete(stream)
        await bus.close()
