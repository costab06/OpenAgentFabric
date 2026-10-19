
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


async def prepare_pending(bus, stream, group, consumer):
    event = make_event()
    message_id = await bus.publish(stream, event)

    async for received_id, _ in bus.subscribe(
        stream=stream,
        group=group,
        consumer=consumer,
        block_ms=100,
    ):
        assert received_id == message_id
        return message_id, event

    raise AssertionError("Message was not delivered")


@pytest.mark.asyncio
async def test_commit_atomically_clears_attempts():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.cleanup.{uuid4().hex}"
    group = "cleanup-test-group"
    consumer = "consumer-a"
    message_id = None

    try:
        message_id, event = await prepare_pending(
            bus, stream, group, consumer
        )

        for expected in range(1, 4):
            assert await bus.begin_attempt(
                stream, group, message_id, consumer
            ) == expected

        assert await bus.get_attempt_count(
            stream, group, message_id
        ) == 3

        result = await bus.commit(
            stream=stream,
            group=group,
            message_id=message_id,
            source_event_id=event.event_id,
            downstream_events=[],
            consumer=consumer,
        )

        assert result is True

        assert await bus.get_attempt_count(
            stream, group, message_id
        ) == 0

        assert await bus.get_pending_message(
            stream, group, message_id
        ) is None

    finally:
        keys = [stream]
        if message_id is not None:
            keys.append(
                bus._attempt_key(stream, group, message_id)
            )
        await bus.redis.delete(*keys)
        await bus.close()


@pytest.mark.asyncio
async def test_dead_letter_atomically_clears_attempts():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.cleanup.{uuid4().hex}"
    dlq_stream = f"{stream}.dlq"
    group = "cleanup-test-group"
    consumer = "consumer-a"
    message_id = None

    try:
        message_id, event = await prepare_pending(
            bus, stream, group, consumer
        )

        for expected in range(1, 6):
            assert await bus.begin_attempt(
                stream, group, message_id, consumer
            ) == expected

        assert await bus.get_attempt_count(
            stream, group, message_id
        ) == 5

        result = await bus.dead_letter(
            stream=stream,
            group=group,
            message_id=message_id,
            source_event_id=event.event_id,
            error="Simulated permanent failure",
            attempts=5,
            agent=AgentName.FETCHER.value,
            dlq_stream=dlq_stream,
            consumer=consumer,
        )

        assert result is True

        assert await bus.get_attempt_count(
            stream, group, message_id
        ) == 0

        assert await bus.get_pending_message(
            stream, group, message_id
        ) is None

        entries = await bus.redis.xrange(dlq_stream)
        assert len(entries) == 1

    finally:
        keys = [stream, dlq_stream]
        if message_id is not None:
            keys.append(
                bus._attempt_key(stream, group, message_id)
            )
        await bus.redis.delete(*keys)
        await bus.close()


@pytest.mark.asyncio
async def test_failed_commit_preserves_attempts():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.cleanup.{uuid4().hex}"
    group = "cleanup-test-group"
    consumer = "consumer-a"
    message_id = None

    try:
        message_id, event = await prepare_pending(
            bus, stream, group, consumer
        )

        await bus.begin_attempt(
            stream, group, message_id, consumer
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
                consumer=consumer,
            )

        assert await bus.get_attempt_count(
            stream, group, message_id
        ) == 1

        assert await bus.get_pending_message(
            stream, group, message_id
        ) is not None

    finally:
        keys = [stream]
        if message_id is not None:
            keys.append(
                bus._attempt_key(stream, group, message_id)
            )
        await bus.redis.delete(*keys)
        await bus.close()
