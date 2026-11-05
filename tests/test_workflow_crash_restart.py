"""Redis-backed workflow crash-window tests (requires Redis at localhost:6380).

Uses a fresh stream/group and deterministic workflow step per test. No Redis FLUSHDB.
"""
from uuid import uuid4

import pytest
import pytest_asyncio
from redis.exceptions import ResponseError

from shared.eventbus.events import EventEnvelope
from shared.eventbus.redis import RedisEventBus
from shared.workflow.registry import StepCommit, StepRegistry

URL = "redis://localhost:6380/14"


@pytest_asyncio.fixture
async def scenario():
    bus = RedisEventBus(URL)
    try:
        await bus.redis.ping()
    except Exception:
        await bus.close()
        pytest.skip("Redis unavailable on localhost:6380")
    suffix = uuid4().hex
    stream, group = f"oaf.crash.{suffix}", f"oaf.crash.group.{suffix}"
    correlation_id = uuid4()
    source = EventEnvelope(event_type="crash.request", source_agent="origin",
                           correlation_id=correlation_id, payload={"input": 1})
    child = EventEnvelope(event_type="crash.result", source_agent="worker",
                          correlation_id=correlation_id, causation_id=source.event_id,
                          payload={"output": 2})
    registry = StepRegistry(bus.redis)
    step = str(source.event_id)
    key = registry.key(correlation_id, "worker", step)
    terminal = f"oaf:terminal:{stream}:{group}:{source.event_id}"
    message_id = None
    try:
        message_id = await bus.publish(stream, source)
        async for received_id, _ in bus.subscribe(stream, group, "worker-a", count=1, block_ms=100):
            assert received_id == message_id
            break
        yield bus, registry, stream, group, message_id, source, child, step
    finally:
        await bus.redis.delete(stream, key, terminal,
                               bus._attempt_key(stream, group, message_id))
        await bus.close()


def completion(source, step, token):
    return StepCommit(source.correlation_id, "worker", step, "input-fingerprint", token,
                      {"event_id": str(source.event_id)})


@pytest.mark.asyncio
async def test_crash_before_handler_commit_reclaims_and_runs_once(scenario):
    bus, registry, stream, group, mid, source, child, step = scenario
    first = await registry.claim(source.correlation_id, "worker", step, "input-fingerprint")
    assert first.status == "claimed"
    # Simulate abrupt process death: no release and no event commit.
    assert await bus.get_terminal_state(stream, group, source.event_id) is None
    assert len(await bus.redis.xrange(stream)) == 1
    # Advance the Redis-owned lease instead of sleeping for 30 seconds.
    await bus.redis.hset(registry.key(source.correlation_id, "worker", step), "lease_until", 0)
    _, recovered = await bus.reclaim_pending_batch(stream, group, "worker-b", min_idle_ms=0)
    assert [item[0] for item in recovered] == [mid]
    second = await registry.claim(source.correlation_id, "worker", step, "input-fingerprint")
    assert second.status == "claimed" and second.token != first.token
    assert await bus.commit(stream, group, mid, source.event_id, [child], consumer="worker-b",
                            step_commit=completion(source, step, second.token))
    assert len(await bus.redis.xrange(stream)) == 2
    assert (await registry.get(source.correlation_id, "worker", step))["status"] == "completed"


@pytest.mark.asyncio
async def test_stale_worker_cannot_commit_after_reclaim(scenario):
    bus, registry, stream, group, mid, source, child, step = scenario
    first = await registry.claim(source.correlation_id, "worker", step, "input-fingerprint")
    await bus.redis.hset(registry.key(source.correlation_id, "worker", step), "lease_until", 0)
    await bus.reclaim_pending_batch(stream, group, "worker-b", min_idle_ms=0)
    second = await registry.claim(source.correlation_id, "worker", step, "input-fingerprint")
    with pytest.raises(ResponseError, match="OAF_STEP_OWNERSHIP_LOST"):
        await bus.commit(stream, group, mid, source.event_id, [child], consumer="worker-b",
                         step_commit=completion(source, step, first.token))
    assert len(await bus.redis.xrange(stream)) == 1
    assert (await bus.redis.xpending(stream, group))["pending"] == 1
    assert await bus.commit(stream, group, mid, source.event_id, [child], consumer="worker-b",
                            step_commit=completion(source, step, second.token))


@pytest.mark.asyncio
async def test_crash_after_commit_replay_never_republishes(scenario):
    bus, registry, stream, group, mid, source, child, step = scenario
    claim = await registry.claim(source.correlation_id, "worker", step, "input-fingerprint")
    context = completion(source, step, claim.token)
    assert await bus.commit(stream, group, mid, source.event_id, [child], consumer="worker-a",
                            step_commit=context)
    # Simulate the process dying before it receives the successful response.
    assert await bus.get_terminal_state(stream, group, source.event_id) == "committed"
    assert (await registry.claim(source.correlation_id, "worker", step, "input-fingerprint")).status == "completed"
    assert await bus.commit(stream, group, mid, source.event_id, [child], consumer="worker-a",
                            step_commit=context) is False
    assert len(await bus.redis.xrange(stream)) == 2
    assert (await bus.redis.xpending(stream, group))["pending"] == 0


@pytest.mark.asyncio
async def test_failed_commit_validation_leaves_step_pending_for_restart(scenario):
    bus, registry, stream, group, mid, source, child, step = scenario
    claim = await registry.claim(source.correlation_id, "worker", step, "input-fingerprint")
    # Handler returned an invalid downstream event: Lua must reject all writes.
    invalid = child.model_copy(update={"correlation_id": uuid4()})
    with pytest.raises(ResponseError, match="OAF_CORRELATION_ID_MISMATCH"):
        await bus.commit(stream, group, mid, source.event_id, [invalid], consumer="worker-a",
                         step_commit=completion(source, step, claim.token))
    assert await bus.get_terminal_state(stream, group, source.event_id) is None
    assert len(await bus.redis.xrange(stream)) == 1
    assert (await registry.get(source.correlation_id, "worker", step))["status"] == "running"
    await bus.redis.hset(registry.key(source.correlation_id, "worker", step), "lease_until", 0)
    await bus.reclaim_pending_batch(stream, group, "worker-b", min_idle_ms=0)
    new = await registry.claim(source.correlation_id, "worker", step, "input-fingerprint")
    assert new.status == "claimed"
    assert await bus.commit(stream, group, mid, source.event_id, [child], consumer="worker-b",
                            step_commit=completion(source, step, new.token))
    assert len(await bus.redis.xrange(stream)) == 2
