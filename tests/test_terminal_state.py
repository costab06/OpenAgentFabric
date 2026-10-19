
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
async def test_commit_prevents_subsequent_dead_letter():
    bus = RedisEventBus(REDIS_URL)
    suffix = uuid4().hex
    stream = f"oaf.test.terminal.{suffix}"
    dlq_stream = f"oaf.test.terminal.dlq.{suffix}"
    group = "terminal-test-group"
    event = make_event()

    terminal_key = (
        f"oaf:terminal:{stream}:{group}:{event.event_id}"
    )

    try:
        message_id = await prepare_pending(
            bus, stream, group, event
        )

        committed = await bus.commit(
            stream=stream,
            group=group,
            message_id=message_id,
            source_event_id=event.event_id,
            downstream_events=[],
        )

        assert committed is True
        assert await bus.redis.get(terminal_key) == "committed"

        with pytest.raises(
            ResponseError,
            match="OAF_ALREADY_COMMITTED",
        ):
            await bus.dead_letter(
                stream=stream,
                group=group,
                message_id=message_id,
                source_event_id=event.event_id,
                error="Late failure",
                attempts=5,
                agent="fetcher",
                dlq_stream=dlq_stream,
            )

        assert await bus.redis.xlen(dlq_stream) == 0

    finally:
        await bus.redis.delete(stream, dlq_stream, terminal_key)
        await bus.close()


@pytest.mark.asyncio
async def test_dead_letter_prevents_subsequent_commit():
    bus = RedisEventBus(REDIS_URL)
    suffix = uuid4().hex
    stream = f"oaf.test.terminal.{suffix}"
    dlq_stream = f"oaf.test.terminal.dlq.{suffix}"
    group = "terminal-test-group"
    event = make_event()

    terminal_key = (
        f"oaf:terminal:{stream}:{group}:{event.event_id}"
    )

    try:
        message_id = await prepare_pending(
            bus, stream, group, event
        )

        moved = await bus.dead_letter(
            stream=stream,
            group=group,
            message_id=message_id,
            source_event_id=event.event_id,
            error="Permanent failure",
            attempts=5,
            agent="fetcher",
            dlq_stream=dlq_stream,
        )

        assert moved is True
        assert await bus.redis.get(terminal_key) == "dead_lettered"

        with pytest.raises(
            ResponseError,
            match="OAF_ALREADY_DEAD_LETTERED",
        ):
            await bus.commit(
                stream=stream,
                group=group,
                message_id=message_id,
                source_event_id=event.event_id,
                downstream_events=[],
            )

        assert await bus.redis.xlen(stream) == 1
        assert await bus.redis.xlen(dlq_stream) == 1

    finally:
        await bus.redis.delete(stream, dlq_stream, terminal_key)
        await bus.close()


@pytest.mark.asyncio
async def test_terminal_state_is_group_specific():
    bus = RedisEventBus(REDIS_URL)
    suffix = uuid4().hex
    stream = f"oaf.test.terminal.{suffix}"
    group_a = "terminal-group-a"
    group_b = "terminal-group-b"
    event = make_event()

    key_a = f"oaf:terminal:{stream}:{group_a}:{event.event_id}"
    key_b = f"oaf:terminal:{stream}:{group_b}:{event.event_id}"

    try:
        message_id = await prepare_pending(
            bus, stream, group_a, event
        )

        # Create a second consumer group and deliver the same event.
        await bus.redis.xgroup_create(
            name=stream,
            groupname=group_b,
            id="0",
        )

        messages = await bus.redis.xreadgroup(
            groupname=group_b,
            consumername="consumer-b",
            streams={stream: ">"},
            count=1,
        )

        assert messages

        assert await bus.commit(
            stream, group_a, message_id, event.event_id, []
        )

        assert await bus.commit(
            stream, group_b, message_id, event.event_id, []
        )

        assert await bus.redis.get(key_a) == "committed"
        assert await bus.redis.get(key_b) == "committed"

    finally:
        await bus.redis.delete(stream, key_a, key_b)
        await bus.close()
