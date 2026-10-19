
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
async def test_dead_letter_moves_message_atomically():
    bus = RedisEventBus(REDIS_URL)
    suffix = uuid4().hex
    stream = f"oaf.test.dlq.{suffix}"
    dlq_stream = f"oaf.test.dlq.destination.{suffix}"
    group = "dlq-test-group"
    event = make_event()

    try:
        message_id = await prepare_pending(
            bus, stream, group, event
        )

        moved = await bus.dead_letter(
            stream=stream,
            group=group,
            message_id=message_id,
            source_event_id=event.event_id,
            error="Simulated permanent failure",
            attempts=5,
            agent="fetcher",
            dlq_stream=dlq_stream,
        )

        assert moved is True
        assert await bus.redis.xlen(dlq_stream) == 1

        records = await bus.redis.xrange(dlq_stream)
        fields = records[0][1]

        assert fields["source_message_id"] == message_id
        assert fields["error"] == "Simulated permanent failure"
        assert fields["attempts"] == "5"
        assert fields["agent"] == "fetcher"

        pending = await bus.redis.xpending(stream, group)
        assert pending["pending"] == 0

        # The second call must not create another DLQ record.
        moved_again = await bus.dead_letter(
            stream=stream,
            group=group,
            message_id=message_id,
            source_event_id=event.event_id,
            error="Simulated permanent failure",
            attempts=5,
            agent="fetcher",
            dlq_stream=dlq_stream,
        )

        assert moved_again is False
        assert await bus.redis.xlen(dlq_stream) == 1

    finally:
        await bus.redis.delete(
            stream,
            dlq_stream,
            f"oaf:deadlettered:{stream}:{group}:{event.event_id}",
        )
        await bus.close()


@pytest.mark.asyncio
async def test_dead_letter_rejects_wrong_event_id():
    bus = RedisEventBus(REDIS_URL)
    suffix = uuid4().hex
    stream = f"oaf.test.dlq.{suffix}"
    dlq_stream = f"oaf.test.dlq.destination.{suffix}"
    group = "dlq-test-group"
    event = make_event()

    try:
        message_id = await prepare_pending(
            bus, stream, group, event
        )

        with pytest.raises(
            ResponseError,
            match="OAF_SOURCE_EVENT_ID_MISMATCH",
        ):
            await bus.dead_letter(
                stream=stream,
                group=group,
                message_id=message_id,
                source_event_id=uuid4(),
                error="Test failure",
                attempts=5,
                agent="fetcher",
                dlq_stream=dlq_stream,
            )

        assert await bus.redis.xlen(dlq_stream) == 0

        pending = await bus.redis.xpending(stream, group)
        assert pending["pending"] == 1

    finally:
        await bus.redis.delete(stream, dlq_stream)
        await bus.close()


@pytest.mark.asyncio
async def test_dead_letter_rejects_nonpending_message():
    bus = RedisEventBus(REDIS_URL)
    suffix = uuid4().hex
    stream = f"oaf.test.dlq.{suffix}"
    dlq_stream = f"oaf.test.dlq.destination.{suffix}"
    group = "dlq-test-group"
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
            await bus.dead_letter(
                stream=stream,
                group=group,
                message_id=message_id,
                source_event_id=event.event_id,
                error="Test failure",
                attempts=5,
                agent="fetcher",
                dlq_stream=dlq_stream,
            )

        assert await bus.redis.xlen(dlq_stream) == 0

    finally:
        await bus.redis.delete(stream, dlq_stream)
        await bus.close()
