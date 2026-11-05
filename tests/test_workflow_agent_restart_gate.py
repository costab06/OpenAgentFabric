"""Real-Redis end-to-end BaseAgent recovery gates (Redis at localhost:6380)."""
import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from shared.agent.base import BaseAgent
from shared.correlation.registry import CorrelationRegistry
from shared.eventbus.events import EventEnvelope
from shared.eventbus.redis import RedisEventBus
from shared.workflow.registry import StepRegistry

URL = 'redis://localhost:6380/14'


class GateAgent(BaseAgent):
    def __init__(self, bus, correlations, stream, consumer, *, entered=None, release=None):
        super().__init__(
            name='restart-gate', event_bus=bus, correlation_registry=correlations,
            subscribed_event_types={'gate.request'}, stream=stream, consumer=consumer,
            recovery_interval=0.02, min_idle_ms=0, heartbeat_interval=0.02,
        )
        self.entered = entered
        self.release = release
        self.calls = 0

    async def handle_event(self, event):
        self.calls += 1
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            await self.release.wait()
        return [EventEnvelope(
            event_type='gate.finished', source_agent=str(self.name),
            correlation_id=event.correlation_id, causation_id=event.event_id,
            payload={'done': True},
        )]


async def until(predicate, timeout=3):
    async def wait():
        while not await predicate():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(wait(), timeout)


async def setup(bus, stream):
    correlations = CorrelationRegistry(bus.redis)
    record = await correlations.create('restart-gate', datetime.now(timezone.utc), {'stream': stream})
    event = EventEnvelope(event_type='gate.request', source_agent='origin', correlation_id=record.correlation_id, payload={})
    message_id = await bus.publish(stream, event)
    return correlations, event, message_id


async def committed(bus, agent, event):
    return await bus.get_terminal_state(agent.stream, agent.group, event.event_id) == 'committed'


async def cleanup(bus, stream, agent, event, message_id):
    await bus.redis.delete(
        stream,
        StepRegistry.key(event.correlation_id, str(agent.name), str(event.event_id)),
        f'oaf:terminal:{stream}:{agent.group}:{event.event_id}',
        bus._attempt_key(stream, agent.group, message_id),
        f'oaf:correlation:{event.correlation_id}',
    )
    await bus.close()


@pytest.mark.asyncio
async def test_running_agent_restarts_and_recovers_unfinished_message():
    bus = RedisEventBus(URL)
    await bus.redis.ping()
    stream = f'oaf.gate.restart.{uuid4().hex}'
    correlations, event, message_id = await setup(bus, stream)
    entered, never_release = asyncio.Event(), asyncio.Event()
    original = GateAgent(bus, correlations, stream, 'worker-a', entered=entered, release=never_release)
    replacement = GateAgent(bus, correlations, stream, 'worker-b')
    original_task = asyncio.create_task(original.run())
    replacement_task = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        original_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await original_task
        assert await bus.get_pending_message(stream, original.group, message_id) is not None
        assert await bus.get_terminal_state(stream, original.group, event.event_id) is None

        # Emulate elapsed wall-clock lease time after process death without
        # sleeping 30 seconds. No worker is allowed to renew this claim.
        step_key = StepRegistry.key(event.correlation_id, str(original.name), str(event.event_id))
        await bus.redis.hset(step_key, 'lease_until', '1')
        replacement_task = asyncio.create_task(replacement.run())
        await until(lambda: committed(bus, replacement, event))
        assert replacement.calls == 1
        assert original.calls == 1
        assert len(await bus.redis.xrange(stream)) == 2
        assert await bus.get_pending_message(stream, replacement.group, message_id) is None
        assert (await replacement.step_registry.get(event.correlation_id, str(replacement.name), str(event.event_id)))['status'] == 'completed'
    finally:
        for task in (original_task, replacement_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        await cleanup(bus, stream, original, event, message_id)


@pytest.mark.asyncio
async def test_running_agent_reconciles_lost_commit_response_and_restart():
    bus = RedisEventBus(URL)
    await bus.redis.ping()
    stream = f'oaf.gate.ambiguous.{uuid4().hex}'
    correlations, event, message_id = await setup(bus, stream)
    original = GateAgent(bus, correlations, stream, 'worker-a')
    replacement = GateAgent(bus, correlations, stream, 'worker-b')
    actual_commit = bus.commit
    commit_calls = 0

    async def commit_then_lose_response(**kwargs):
        nonlocal commit_calls
        commit_calls += 1
        result = await actual_commit(**kwargs)
        assert result is True
        raise ConnectionError('commit succeeded but response was lost')

    bus.commit = commit_then_lose_response
    original_task = asyncio.create_task(original.run())
    replacement_task = None
    try:
        await until(lambda: committed(bus, original, event))
        await until(lambda: _not_pending(bus, original, message_id))
        assert commit_calls == 1
        assert original.calls == 1
        assert len(await bus.redis.xrange(stream)) == 2
        record = await original.step_registry.get(event.correlation_id, str(original.name), str(event.event_id))
        assert record['status'] == 'completed'

        original_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await original_task
        replacement_task = asyncio.create_task(replacement.run())
        await asyncio.sleep(0.1)
        assert replacement.calls == 0
        assert len(await bus.redis.xrange(stream)) == 2
        assert commit_calls == 1
    finally:
        for task in (original_task, replacement_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        await cleanup(bus, stream, original, event, message_id)


async def _not_pending(bus, agent, message_id):
    return await bus.get_pending_message(agent.stream, agent.group, message_id) is None
