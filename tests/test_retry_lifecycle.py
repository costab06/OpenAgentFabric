
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from shared.agent.base import BaseAgent
from shared.correlation.registry import CorrelationRegistry
from shared.eventbus.events import (
    DocumentDiscoveredEvent,
    DocumentDiscoveredPayload,
    EventType,
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


class RetryLifecycleAgent(BaseAgent):
    def __init__(
        self,
        bus,
        registry,
        stream,
        consumer,
        failures,
    ):
        super().__init__(
            name=AgentName.FETCHER,
            event_bus=bus,
            correlation_registry=registry,
            subscribed_event_types={
                EventType.DOCUMENT_DISCOVERED
            },
            stream=stream,
            consumer=consumer,
            max_attempts=5,
            dlq_stream=f"{stream}.dlq",
            min_idle_ms=0,
        )

        self.failures_remaining = failures
        self.handler_calls = 0
        self.received_events = []

    async def handle_event(self, event):
        self.handler_calls += 1
        self.received_events.append(event)

        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise ValueError("Simulated processing failure")

        return []


async def prepare_event(bus, registry, stream, group):
    # CorrelationRegistry.create() derives and persists
    # a correlation record, returning a dictionary.
    correlation_record = await registry.create(
        trigger_type="retry-lifecycle-test",
        trigger_timestamp=datetime.now(timezone.utc),
        business_fields={
            "source": "retry-lifecycle-test",
            "stream": stream,
        },
    )

    # Extract the UUID rather than assigning the
    # entire correlation record to the event.
    correlation_id = UUID(
        str(correlation_record.correlation_id)
    )

    event = make_event().model_copy(
        update={"correlation_id": correlation_id}
    )

    message_id = await bus.publish(stream, event)

    async for received_id, received_event in bus.subscribe(
        stream=stream,
        group=group,
        consumer="consumer-a",
        block_ms=100,
    ):
        assert received_id == message_id
        assert received_event.event_id == event.event_id
        assert received_event.correlation_id == correlation_id

        return message_id, event

    raise AssertionError("Event was not delivered")


async def reclaim_one(bus, stream, group, consumer):
    _, recovered = await bus.reclaim_pending_batch(
        stream=stream,
        group=group,
        consumer=consumer,
        cursor="0-0",
        min_idle_ms=0,
        count=10,
    )

    assert len(recovered) == 1
    return recovered[0]


async def cleanup(bus, stream, group, message_id, event):
    keys = [
        stream,
        f"{stream}.dlq",
        bus._attempt_key(stream, group, message_id),
        f"oaf:terminal:{stream}:{group}:{event.event_id}",
    ]

    await bus.redis.delete(*keys)
    await bus.close()


@pytest.mark.asyncio
async def test_five_failures_dead_letter_once():
    bus = RedisEventBus(REDIS_URL)
    registry = CorrelationRegistry(bus.redis)

    stream = f"oaf.test.lifecycle.{uuid4().hex}"

    agent = RetryLifecycleAgent(
        bus=bus,
        registry=registry,
        stream=stream,
        consumer="consumer-a",
        failures=5,
    )

    message_id = None
    event = None

    try:
        message_id, event = await prepare_event(
            bus,
            registry,
            stream,
            agent.group,
        )

        # First processing attempt fails.
        await agent.process_message(message_id, event)

        assert agent.handler_calls == 1

        assert await bus.get_attempt_count(
            stream,
            agent.group,
            message_id,
        ) == 1

        # Reclaim the pending message and retry.
        # Attempts 2 through 5 all fail.
        for expected_attempt in range(2, 6):
            recovered_id, recovered_event = await reclaim_one(
                bus,
                stream,
                agent.group,
                agent.consumer,
            )

            assert recovered_id == message_id
            assert recovered_event.event_id == event.event_id
            assert (
                recovered_event.correlation_id
                == event.correlation_id
            )
            assert (
                recovered_event.causation_id
                == event.causation_id
            )

            await agent.process_message(
                recovered_id,
                recovered_event,
            )

            assert agent.handler_calls == expected_attempt

        # After the fifth failure, the source message
        # must no longer be pending.
        assert await bus.get_pending_message(
            stream,
            agent.group,
            message_id,
        ) is None

        # Atomic dead-letter operation clears attempts.
        assert await bus.get_attempt_count(
            stream,
            agent.group,
            message_id,
        ) == 0

        # Exactly one DLQ entry exists.
        entries = await bus.redis.xrange(
            f"{stream}.dlq"
        )

        assert len(entries) == 1

        # A second dead-letter attempt is idempotent.
        result = await bus.dead_letter(
            stream=stream,
            group=agent.group,
            message_id=message_id,
            source_event_id=event.event_id,
            error="Duplicate failure",
            attempts=5,
            agent=agent.name.value,
            dlq_stream=f"{stream}.dlq",
            consumer=agent.consumer,
        )

        assert result is False

        entries = await bus.redis.xrange(
            f"{stream}.dlq"
        )

        assert len(entries) == 1

        assert agent.handler_calls == 5

    finally:
        if message_id is not None and event is not None:
            await cleanup(
                bus,
                stream,
                agent.group,
                message_id,
                event,
            )
        else:
            await bus.redis.delete(
                stream,
                f"{stream}.dlq",
            )
            await bus.close()


@pytest.mark.asyncio
async def test_success_on_fifth_attempt_commits():
    bus = RedisEventBus(REDIS_URL)
    registry = CorrelationRegistry(bus.redis)

    stream = f"oaf.test.lifecycle.{uuid4().hex}"

    agent = RetryLifecycleAgent(
        bus=bus,
        registry=registry,
        stream=stream,
        consumer="consumer-a",
        failures=4,
    )

    message_id = None
    event = None

    try:
        message_id, event = await prepare_event(
            bus,
            registry,
            stream,
            agent.group,
        )

        # First processing attempt fails.
        await agent.process_message(message_id, event)

        assert agent.handler_calls == 1

        # Attempts 2, 3, and 4 fail.
        # Attempt 5 succeeds.
        for expected_attempt in range(2, 6):
            recovered_id, recovered_event = await reclaim_one(
                bus,
                stream,
                agent.group,
                agent.consumer,
            )

            assert recovered_id == message_id
            assert recovered_event.event_id == event.event_id
            assert (
                recovered_event.correlation_id
                == event.correlation_id
            )

            await agent.process_message(
                recovered_id,
                recovered_event,
            )

            assert agent.handler_calls == expected_attempt

        assert agent.handler_calls == 5

        # Successful commit acknowledges the source.
        assert await bus.get_pending_message(
            stream,
            agent.group,
            message_id,
        ) is None

        # Successful commit atomically clears attempts.
        assert await bus.get_attempt_count(
            stream,
            agent.group,
            message_id,
        ) == 0

        # No dead-letter entry should exist.
        entries = await bus.redis.xrange(
            f"{stream}.dlq"
        )

        assert len(entries) == 0

        # Repeating the commit must not produce
        # another terminal operation.
        result = await bus.commit(
            stream=stream,
            group=agent.group,
            message_id=message_id,
            source_event_id=event.event_id,
            downstream_events=[],
            consumer=agent.consumer,
        )

        assert result is False

    finally:
        if message_id is not None and event is not None:
            await cleanup(
                bus,
                stream,
                agent.group,
                message_id,
                event,
            )
        else:
            await bus.redis.delete(
                stream,
                f"{stream}.dlq",
            )
            await bus.close()
