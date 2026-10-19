
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

    raise AssertionError("No message received")


@pytest.mark.asyncio
async def test_attempt_count_starts_at_zero():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.attempts.{uuid4().hex}"
    group = "attempt-test-group"

    try:
        message_id = await prepare_pending(
            bus, stream, group
        )

        assert await bus.get_attempt_count(
            stream, group, message_id
        ) == 0

    finally:
        await bus.redis.delete(
            stream,
            bus._attempt_key(stream, group, message_id),
        )
        await bus.close()


@pytest.mark.asyncio
async def test_attempt_count_increments_and_persists():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.attempts.{uuid4().hex}"
    group = "attempt-test-group"

    try:
        message_id = await prepare_pending(
            bus, stream, group
        )

        for expected in range(1, 6):
            attempts = await bus.begin_attempt(
                stream, group, message_id, "consumer-a"
            )
            assert attempts == expected

        # A new RedisEventBus instance sees the same count.
        second_bus = RedisEventBus(REDIS_URL)

        try:
            assert await second_bus.get_attempt_count(
                stream, group, message_id
            ) == 5
        finally:
            await second_bus.close()

    finally:
        await bus.redis.delete(
            stream,
            bus._attempt_key(stream, group, message_id),
        )
        await bus.close()


@pytest.mark.asyncio
async def test_wrong_consumer_cannot_increment_attempts():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.attempts.{uuid4().hex}"
    group = "attempt-test-group"

    try:
        message_id = await prepare_pending(
            bus, stream, group
        )

        with pytest.raises(
            ResponseError,
            match="OAF_CONSUMER_OWNERSHIP_MISMATCH",
        ):
            await bus.begin_attempt(
                stream, group, message_id, "consumer-b"
            )

        assert await bus.get_attempt_count(
            stream, group, message_id
        ) == 0

    finally:
        await bus.redis.delete(
            stream,
            bus._attempt_key(stream, group, message_id),
        )
        await bus.close()


@pytest.mark.asyncio
async def test_reclaimed_message_preserves_attempt_count():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.attempts.{uuid4().hex}"
    group = "attempt-test-group"

    try:
        message_id = await prepare_pending(
            bus, stream, group
        )

        assert await bus.begin_attempt(
            stream, group, message_id, "consumer-a"
        ) == 1

        # Reclaiming changes ownership, not the attempt count.
        _, recovered = await bus.reclaim_pending_batch(
            stream=stream,
            group=group,
            consumer="consumer-b",
            min_idle_ms=0,
        )

        assert len(recovered) == 1

        assert await bus.get_attempt_count(
            stream, group, message_id
        ) == 1

        assert await bus.begin_attempt(
            stream, group, message_id, "consumer-b"
        ) == 2

    finally:
        await bus.redis.delete(
            stream,
            bus._attempt_key(stream, group, message_id),
        )
        await bus.close()


@pytest.mark.asyncio
async def test_attempt_count_can_be_cleared():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.attempts.{uuid4().hex}"
    group = "attempt-test-group"

    try:
        message_id = await prepare_pending(
            bus, stream, group
        )

        assert await bus.begin_attempt(
            stream, group, message_id, "consumer-a"
        ) == 1

        await bus.clear_attempts(
            stream, group, message_id
        )

        assert await bus.get_attempt_count(
            stream, group, message_id
        ) == 0

    finally:
        await bus.redis.delete(
            stream,
            bus._attempt_key(stream, group, message_id),
        )
        await bus.close()
