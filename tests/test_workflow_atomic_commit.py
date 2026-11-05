"""Atomic workflow commit tests; requires local Redis on port 6380."""
from uuid import uuid4
import pytest
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from shared.eventbus.events import EventEnvelope
from shared.eventbus.redis import RedisEventBus
from shared.workflow import StepRegistry, StepCommit

URL = "redis://localhost:6380/14"  # isolated Redis database; uses unique stream/key names

@pytest.mark.asyncio
async def test_step_completion_and_event_commit_atomic():
    bus = RedisEventBus(URL)
    redis = bus.redis
    try:
        await redis.ping()
    except Exception:
        pytest.skip("Redis unavailable")
    suffix = uuid4().hex
    stream, group, consumer = f"test.oaf.workflow.{suffix}", f"test.workflow.{suffix}", "worker"
    cid = uuid4()
    source = EventEnvelope(event_type="test.requested", source_agent="test.origin", correlation_id=cid, payload={})
    child = EventEnvelope(event_type="test.completed", source_agent="test.worker", correlation_id=cid, causation_id=source.event_id, payload={})
    registry = StepRegistry(redis)
    key = registry.key(cid, "worker", "compute")
    terminal = f"oaf:terminal:{stream}:{group}:{source.event_id}"
    try:
        await bus.publish(stream, source)
        async for message_id, _ in bus.subscribe(stream, group, consumer, count=1, block_ms=100):
            break
        claim = await registry.claim(cid, "worker", "compute", "input-hash", lease_ms=60000)
        assert claim.status == "claimed"
        completion = StepCommit(cid, "worker", "compute", "input-hash", claim.token, {"ok": True})
        assert await bus.commit(stream, group, message_id, source.event_id, [child], consumer=consumer, step_commit=completion)
        assert (await registry.get(cid, "worker", "compute"))["result"] == {"ok": True}
        assert len(await redis.xrange(stream)) == 2
        assert not await bus.commit(stream, group, message_id, source.event_id, [child], consumer=consumer, step_commit=completion)
        assert len(await redis.xrange(stream)) == 2
    finally:
        await redis.delete(stream, key, terminal, bus._attempt_key(stream, group, message_id))
        await bus.close()

@pytest.mark.asyncio
async def test_stale_step_token_rejected_without_side_effects():
    bus = RedisEventBus(URL)
    redis = bus.redis
    try:
        await redis.ping()
    except Exception:
        pytest.skip("Redis unavailable")
    suffix = uuid4().hex
    stream, group, consumer = f"test.oaf.workflow.{suffix}", f"test.workflow.{suffix}", "worker"
    cid = uuid4()
    source = EventEnvelope(event_type="test.requested", source_agent="test.origin", correlation_id=cid, payload={})
    registry = StepRegistry(redis)
    key = registry.key(cid, "worker", "compute")
    terminal = f"oaf:terminal:{stream}:{group}:{source.event_id}"
    message_id = None
    try:
        await bus.publish(stream, source)
        async for message_id, _ in bus.subscribe(stream, group, consumer, count=1, block_ms=100):
            break
        claim = await registry.claim(cid, "worker", "compute", "input-hash", lease_ms=60000)
        invalid = StepCommit(cid, "worker", "compute", "input-hash", "wrong-token", {"ok": True})
        with pytest.raises(ResponseError, match="OAF_STEP_OWNERSHIP_LOST"):
            await bus.commit(stream, group, message_id, source.event_id, [], consumer=consumer, step_commit=invalid)
        assert len(await redis.xrange(stream)) == 1
        assert (await redis.xpending(stream, group))["pending"] == 1
        assert (await registry.get(cid, "worker", "compute"))["status"] == "running"
        assert await registry.renew(cid, "worker", "compute", "input-hash", claim.token)
    finally:
        await redis.delete(stream, key, terminal, bus._attempt_key(stream, group, message_id))
        await bus.close()
