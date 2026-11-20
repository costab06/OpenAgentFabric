"""Redis-backed tests for terminal marker delivery identity (Redis :6380)."""
import json
from uuid import uuid4

import pytest
from redis.exceptions import ResponseError
from shared.eventbus.redis import RedisEventBus


@pytest.mark.asyncio
async def test_terminal_reconciliation_requires_original_message_identity():
    bus = RedisEventBus("redis://localhost:6380/0")
    stream = f"oaf.test.terminal.identity.{uuid4().hex}"
    group, consumer = "identity-test", "worker-a"
    event_id = str(uuid4())
    terminal = f"oaf:terminal:{stream}:{group}:{event_id}"
    companion = bus._terminal_message_key(stream, group, event_id)
    try:
        await bus.redis.xgroup_create(stream, group, id="0", mkstream=True)

        async def pending(eid):
            mid = await bus.redis.xadd(stream, {"event": json.dumps({"event_id": eid})})
            await bus.redis.xreadgroup(group, consumer, {stream: ">"}, count=1)
            return mid

        async def reconcile(mid, eid=event_id, owner=consumer):
            return await bus.reconcile_terminal_pending(
                stream=stream, group=group, message_id=mid,
                source_event_id=eid, consumer=owner,
            )

        first = await pending(event_id)
        with pytest.raises(ResponseError, match="OAF_NOT_TERMINAL"):
            await reconcile(first)
        await bus.redis.set(terminal, "committed")
        with pytest.raises(ResponseError, match="OAF_TERMINAL_MESSAGE_ID_MISSING"):
            await reconcile(first)
        await bus.redis.set(companion, first)
        assert await reconcile(first) is True
        assert await bus.get_pending_message(stream, group, first) is None

        duplicate = await pending(event_id)
        with pytest.raises(ResponseError, match="OAF_TERMINAL_MESSAGE_ID_MISMATCH"):
            await reconcile(duplicate)
        assert await bus.get_pending_message(stream, group, duplicate) is not None

        other_id = str(uuid4())
        other = await pending(other_id)
        await bus.redis.set(f"oaf:terminal:{stream}:{group}:{other_id}", "dead_lettered")
        await bus.redis.set(bus._terminal_message_key(stream, group, other_id), other)
        await bus.redis.xclaim(stream, group, "worker-b", 0, [other])
        with pytest.raises(ResponseError, match="OAF_CONSUMER_OWNERSHIP_MISMATCH"):
            await reconcile(other, other_id)
    finally:
        async for key in bus.redis.scan_iter(match=f"oaf:*:{stream}:*"):
            await bus.redis.delete(key)
        await bus.redis.delete(stream)
        await bus.close()
