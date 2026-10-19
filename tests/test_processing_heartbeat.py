
import asyncio
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


async def prepare_pending(bus, stream, group):
    event = make_event()
    message_id = await bus.publish(stream, event)

    async for received_id, _ in bus.subscribe(
        stream=stream,
        group=group,
        consumer="consumer-a",
        block_ms=100,
    ):
        assert received_id == message_id
        return message_id

    raise AssertionError("Message was not delivered")


@pytest.mark.asyncio
async def test_heartbeat_preserves_ownership_and_delivery_count():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.heartbeat.{uuid4().hex}"
    group = "heartbeat-test-group"
    message_id = None

    try:
        message_id = await prepare_pending(
            bus, stream, group
        )

        before = await bus.get_pending_message(
            stream, group, message_id
        )

        assert before.consumer == "consumer-a"
        assert before.delivery_count == 1

        await asyncio.sleep(0.02)

        assert await bus.heartbeat(
            stream, group, message_id, "consumer-a"
        ) is True

        after = await bus.get_pending_message(
            stream, group, message_id
        )

        assert after.consumer == "consumer-a"
        assert after.delivery_count == 1
        assert after.idle_ms < 100

    finally:
        if message_id is not None:
            await bus.redis.delete(stream)
        await bus.close()


@pytest.mark.asyncio
async def test_wrong_consumer_cannot_heartbeat():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.heartbeat.{uuid4().hex}"
    group = "heartbeat-test-group"
    message_id = None

    try:
        message_id = await prepare_pending(
            bus, stream, group
        )

        assert await bus.heartbeat(
            stream, group, message_id, "consumer-b"
        ) is False

        pending = await bus.get_pending_message(
            stream, group, message_id
        )

        assert pending.consumer == "consumer-a"
        assert pending.delivery_count == 1

    finally:
        if message_id is not None:
            await bus.redis.delete(stream)
        await bus.close()


@pytest.mark.asyncio
async def test_previous_owner_cannot_heartbeat_after_reclaim():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.heartbeat.{uuid4().hex}"
    group = "heartbeat-test-group"
    message_id = None

    try:
        message_id = await prepare_pending(
            bus, stream, group
        )

        _, recovered = await bus.reclaim_pending_batch(
            stream=stream,
            group=group,
            consumer="consumer-b",
            min_idle_ms=0,
        )

        assert len(recovered) == 1

        assert await bus.heartbeat(
            stream, group, message_id, "consumer-a"
        ) is False

        assert await bus.heartbeat(
            stream, group, message_id, "consumer-b"
        ) is True

        pending = await bus.get_pending_message(
            stream, group, message_id
        )

        assert pending.consumer == "consumer-b"
        assert pending.delivery_count == 2

    finally:
        if message_id is not None:
            await bus.redis.delete(stream)
        await bus.close()


@pytest.mark.asyncio
async def test_acknowledged_message_cannot_heartbeat():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.heartbeat.{uuid4().hex}"
    group = "heartbeat-test-group"
    message_id = None

    try:
        message_id = await prepare_pending(
            bus, stream, group
        )

        await bus.acknowledge(
            stream, group, message_id
        )

        assert await bus.heartbeat(
            stream, group, message_id, "consumer-a"
        ) is False

    finally:
        if message_id is not None:
            await bus.redis.delete(stream)
        await bus.close()
