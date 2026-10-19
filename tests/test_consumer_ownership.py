
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
    """Publish an event and deliver it to consumer-a."""
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
async def test_wrong_consumer_cannot_commit():
    """
    A consumer that does not own the pending message
    must not be able to commit it.
    """
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.owner.{uuid4().hex}"
    group = "ownership-group"
    event = make_event()

    terminal_key = (
        f"oaf:terminal:{stream}:{group}:{event.event_id}"
    )

    try:
        message_id = await prepare_pending(
            bus, stream, group, event
        )

        # Consumer B must not commit consumer A's message.
        with pytest.raises(
            ResponseError,
            match="OAF_CONSUMER_OWNERSHIP_MISMATCH",
        ):
            await bus.commit(
                stream=stream,
                group=group,
                message_id=message_id,
                source_event_id=event.event_id,
                downstream_events=[],
                consumer="consumer-b",
            )

        # Failed commit must not acknowledge the message.
        pending = await bus.get_pending_message(
            stream, group, message_id
        )

        assert pending is not None
        assert pending.consumer == "consumer-a"
        assert await bus.redis.get(terminal_key) is None

        # The actual owner can commit successfully.
        committed = await bus.commit(
            stream=stream,
            group=group,
            message_id=message_id,
            source_event_id=event.event_id,
            downstream_events=[],
            consumer="consumer-a",
        )

        assert committed is True
        assert await bus.redis.get(terminal_key) == "committed"

    finally:
        await bus.redis.delete(stream, terminal_key)
        await bus.close()


@pytest.mark.asyncio
async def test_wrong_consumer_cannot_dead_letter():
    """
    A consumer that does not own the pending message
    must not be able to dead-letter it.
    """
    bus = RedisEventBus(REDIS_URL)

    suffix = uuid4().hex
    stream = f"oaf.test.owner.{suffix}"
    dlq_stream = f"oaf.test.owner.dlq.{suffix}"
    group = "ownership-group"
    event = make_event()

    terminal_key = (
        f"oaf:terminal:{stream}:{group}:{event.event_id}"
    )

    try:
        message_id = await prepare_pending(
            bus, stream, group, event
        )

        # Consumer B must not dead-letter consumer A's message.
        with pytest.raises(
            ResponseError,
            match="OAF_CONSUMER_OWNERSHIP_MISMATCH",
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
                consumer="consumer-b",
            )

        # No DLQ entry should have been created.
        assert await bus.redis.xlen(dlq_stream) == 0

        pending = await bus.get_pending_message(
            stream, group, message_id
        )

        assert pending is not None
        assert pending.consumer == "consumer-a"

        # Consumer A can dead-letter its own message.
        moved = await bus.dead_letter(
            stream=stream,
            group=group,
            message_id=message_id,
            source_event_id=event.event_id,
            error="Test failure",
            attempts=5,
            agent="fetcher",
            dlq_stream=dlq_stream,
            consumer="consumer-a",
        )

        assert moved is True
        assert await bus.redis.xlen(dlq_stream) == 1
        assert await bus.redis.get(terminal_key) == "dead_lettered"

    finally:
        await bus.redis.delete(
            stream, dlq_stream, terminal_key
        )
        await bus.close()


@pytest.mark.asyncio
async def test_reclaimed_message_rejects_previous_owner():
    """
    After XAUTOCLAIM transfers ownership from A to B,
    consumer A must no longer be able to commit the message.
    """
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.owner.{uuid4().hex}"
    group = "ownership-group"
    event = make_event()

    terminal_key = (
        f"oaf:terminal:{stream}:{group}:{event.event_id}"
    )

    try:
        message_id = await prepare_pending(
            bus, stream, group, event
        )

        # Consumer B reclaims the pending message.
        _, recovered = await bus.reclaim_pending_batch(
            stream=stream,
            group=group,
            consumer="consumer-b",
            min_idle_ms=0,
        )

        assert len(recovered) == 1
        assert recovered[0][0] == message_id

        # Verify Redis now identifies consumer B as owner.
        pending = await bus.get_pending_message(
            stream, group, message_id
        )

        assert pending is not None
        assert pending.consumer == "consumer-b"
        assert pending.delivery_count == 2

        # The previous owner must be rejected.
        with pytest.raises(
            ResponseError,
            match="OAF_CONSUMER_OWNERSHIP_MISMATCH",
        ):
            await bus.commit(
                stream=stream,
                group=group,
                message_id=message_id,
                source_event_id=event.event_id,
                downstream_events=[],
                consumer="consumer-a",
            )

        # The new owner can commit.
        committed = await bus.commit(
            stream=stream,
            group=group,
            message_id=message_id,
            source_event_id=event.event_id,
            downstream_events=[],
            consumer="consumer-b",
        )

        assert committed is True
        assert await bus.redis.get(terminal_key) == "committed"

    finally:
        await bus.redis.delete(stream, terminal_key)
        await bus.close()
