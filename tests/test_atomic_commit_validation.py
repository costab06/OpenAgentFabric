
from uuid import uuid4

import pytest
from redis.exceptions import ResponseError

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
        consumer="test-consumer",
        block_ms=100,
    ):
        assert received_id == message_id
        return message_id

    raise AssertionError("No message received")


@pytest.mark.asyncio
async def test_commit_rejects_nonexistent_message():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.validation.{uuid4().hex}"
    group = "validation-group"
    event = make_event()

    try:
        await prepare_pending(bus, stream, group, event)

        with pytest.raises(ResponseError, match="OAF_SOURCE_NOT_FOUND"):
            await bus.commit(
                stream=stream,
                group=group,
                message_id="9999999999999-0",
                source_event_id=event.event_id,
                downstream_events=[],
            )

        assert await bus.redis.xlen(stream) == 1

    finally:
        await bus.redis.delete(stream)
        await bus.close()


@pytest.mark.asyncio
async def test_commit_rejects_event_id_mismatch():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.validation.{uuid4().hex}"
    group = "validation-group"
    event = make_event()

    try:
        message_id = await prepare_pending(
            bus, stream, group, event
        )

        with pytest.raises(
            ResponseError,
            match="OAF_SOURCE_EVENT_ID_MISMATCH",
        ):
            await bus.commit(
                stream=stream,
                group=group,
                message_id=message_id,
                source_event_id=uuid4(),
                downstream_events=[],
            )

        assert await bus.redis.xlen(stream) == 1

        pending = await bus.redis.xpending(stream, group)
        assert pending["pending"] == 1

    finally:
        await bus.redis.delete(stream)
        await bus.close()


@pytest.mark.asyncio
async def test_commit_rejects_unclaimed_message():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.validation.{uuid4().hex}"
    group = "validation-group"
    event = make_event()

    try:
        message_id = await prepare_pending(
            bus, stream, group, event
        )

        await bus.acknowledge(stream, group, message_id)

        with pytest.raises(
            ResponseError,
            match="OAF_SOURCE_NOT_PENDING",
        ):
            await bus.commit(
                stream=stream,
                group=group,
                message_id=message_id,
                source_event_id=event.event_id,
                downstream_events=[],
            )

        assert await bus.redis.xlen(stream) == 1

    finally:
        await bus.redis.delete(stream)
        await bus.close()
