
import asyncio
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


class SlowAgent(BaseAgent):
    def __init__(self, bus, registry, stream, consumer):
        super().__init__(
            name=AgentName.FETCHER,
            event_bus=bus,
            correlation_registry=registry,
            subscribed_event_types={
                EventType.DOCUMENT_DISCOVERED
            },
            stream=stream,
            consumer=consumer,
            min_idle_ms=300,
            heartbeat_interval=0.05,
            recovery_interval=0.1,
        )

        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.completed = False

    async def handle_event(self, event):
        self.started.set()
        await self.release.wait()
        self.completed = True
        return []


async def prepare(bus, registry, stream, group):
    record = await registry.create(
        trigger_type="heartbeat-integration-test",
        trigger_timestamp=datetime.now(timezone.utc),
        business_fields={"stream": stream},
    )

    event = DocumentDiscoveredEvent(
        source_agent=AgentName.WATCHER,
        correlation_id=UUID(str(record.correlation_id)),
        payload=DocumentDiscoveredPayload(
            cik="0000320193",
            accession_number="0000320193-24-000123",
            company_name="Apple Inc.",
            filing_type="10-Q",
            filing_date="2024-08-02",
            filing_url="https://www.sec.gov/example",
        ),
    )

    message_id = await bus.publish(stream, event)

    async for received_id, received_event in bus.subscribe(
        stream=stream,
        group=group,
        consumer="consumer-a",
        block_ms=100,
    ):
        assert received_id == message_id
        return message_id, received_event

    raise AssertionError("Message not delivered")


@pytest.mark.asyncio
async def test_active_heartbeat_prevents_reclaim():
    bus = RedisEventBus(REDIS_URL)
    registry = CorrelationRegistry(bus.redis)
    stream = f"oaf.test.agentheartbeat.{uuid4().hex}"

    agent = SlowAgent(
        bus, registry, stream, "consumer-a"
    )

    task = None
    message_id = None
    event = None

    try:
        message_id, event = await prepare(
            bus, registry, stream, agent.group
        )

        task = asyncio.create_task(
            agent.process_message(message_id, event)
        )

        await asyncio.wait_for(
            agent.started.wait(),
            timeout=2,
        )

        # Allow several heartbeats to execute.
        await asyncio.sleep(0.7)

        _, recovered = await bus.reclaim_pending_batch(
            stream=stream,
            group=agent.group,
            consumer="consumer-b",
            min_idle_ms=300,
        )

        assert recovered == []

        pending = await bus.get_pending_message(
            stream, agent.group, message_id
        )

        assert pending.consumer == "consumer-a"
        assert pending.delivery_count == 1

        agent.release.set()
        await asyncio.wait_for(task, timeout=2)

        assert agent.completed is True

        assert await bus.get_pending_message(
            stream, agent.group, message_id
        ) is None

    finally:
        agent.release.set()

        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await bus.redis.delete(stream)
        await bus.close()


@pytest.mark.asyncio
async def test_reclaim_cancels_previous_owner():
    bus = RedisEventBus(REDIS_URL)
    registry = CorrelationRegistry(bus.redis)
    stream = f"oaf.test.agentheartbeat.{uuid4().hex}"

    agent = SlowAgent(
        bus, registry, stream, "consumer-a"
    )

    task = None

    try:
        message_id, event = await prepare(
            bus, registry, stream, agent.group
        )

        task = asyncio.create_task(
            agent.process_message(message_id, event)
        )

        await asyncio.wait_for(
            agent.started.wait(),
            timeout=2,
        )

        # Simulate another consumer taking ownership.
        _, recovered = await bus.reclaim_pending_batch(
            stream=stream,
            group=agent.group,
            consumer="consumer-b",
            min_idle_ms=0,
        )

        assert len(recovered) == 1

        # The next heartbeat detects ownership loss
        # and cancels the old handler.
        await asyncio.wait_for(task, timeout=2)

        assert agent.completed is False

        pending = await bus.get_pending_message(
            stream, agent.group, message_id
        )

        assert pending.consumer == "consumer-b"

    finally:
        agent.release.set()

        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await bus.redis.delete(stream)
        await bus.close()
