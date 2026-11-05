"""Completion indexing is part of the fenced, atomic Redis commit."""
import pytest
from uuid import uuid4
from redis.exceptions import ResponseError
from shared.eventbus.events import EventEnvelope
from shared.eventbus.redis import RedisEventBus
from shared.workflow import StepRegistry, StepCommit

URL = 'redis://localhost:6380/14'

@pytest.mark.asyncio
async def test_completion_index_committed_atomically_and_replay_safe():
    bus = RedisEventBus(URL)
    r = bus.redis
    try:
        await r.ping()
    except Exception:
        pytest.skip('Redis unavailable')
    cid = uuid4()
    stream, group, consumer = f'test.index.{uuid4().hex}', f'test.group.{uuid4().hex}', 'worker'
    source = EventEnvelope(event_type='test.start', source_agent='origin', correlation_id=cid, payload={})
    completion = EventEnvelope(event_type='test.done', source_agent='worker', correlation_id=cid,
                               causation_id=source.event_id, payload={'value': 42})
    index = f'oaf:workflow:completion:{cid}:test.done'
    step_key = StepRegistry.key(cid, 'worker', 'compute')
    terminal = f'oaf:terminal:{stream}:{group}:{source.event_id}'
    mid = None
    try:
        await bus.publish(stream, source)
        async for mid, _ in bus.subscribe(stream, group, consumer, count=1, block_ms=100):
            break
        claim = await StepRegistry(r).claim(cid, 'worker', 'compute', 'input', lease_ms=60000)
        step = StepCommit(cid, 'worker', 'compute', 'input', claim.token, {'ok': True})
        assert await r.get(index) is None
        assert await bus.commit(stream, group, mid, source.event_id, [completion], step,
                                consumer=consumer, completion_event_type='test.done')
        assert await r.get(index) == completion.model_dump_json()
        assert not await bus.commit(stream, group, mid, source.event_id, [completion], step,
                                    consumer=consumer, completion_event_type='test.done')
        assert len(await r.xrange(stream)) == 2
    finally:
        await r.delete(stream, step_key, terminal, index, bus._attempt_key(stream, group, mid or '0-0'))
        await bus.close()

@pytest.mark.asyncio
async def test_missing_completion_type_has_no_side_effects():
    bus = RedisEventBus(URL)
    r = bus.redis
    try:
        await r.ping()
    except Exception:
        pytest.skip('Redis unavailable')
    cid = uuid4()
    stream, group, consumer = f'test.index.{uuid4().hex}', f'test.group.{uuid4().hex}', 'worker'
    source = EventEnvelope(event_type='test.start', source_agent='origin', correlation_id=cid, payload={})
    child = EventEnvelope(event_type='test.other', source_agent='worker', correlation_id=cid,
                          causation_id=source.event_id, payload={})
    index = f'oaf:workflow:completion:{cid}:test.done'
    step_key = StepRegistry.key(cid, 'worker', 'compute')
    terminal = f'oaf:terminal:{stream}:{group}:{source.event_id}'
    mid = None
    try:
        await bus.publish(stream, source)
        async for mid, _ in bus.subscribe(stream, group, consumer, count=1, block_ms=100):
            break
        claim = await StepRegistry(r).claim(cid, 'worker', 'compute', 'input', lease_ms=60000)
        step = StepCommit(cid, 'worker', 'compute', 'input', claim.token, {'ok': True})
        with pytest.raises(ResponseError, match='OAF_COMPLETION_EVENT_MISSING'):
            await bus.commit(stream, group, mid, source.event_id, [child], step,
                             consumer=consumer, completion_event_type='test.done')
        assert await r.get(index) is None
        assert len(await r.xrange(stream)) == 1
        assert (await r.xpending(stream, group))['pending'] == 1
    finally:
        await r.delete(stream, step_key, terminal, index, bus._attempt_key(stream, group, mid or '0-0'))
        await bus.close()
