
from uuid import uuid4

import pytest

from shared.eventbus.redis import RedisEventBus


REDIS_URL = "redis://localhost:6380/0"


@pytest.mark.asyncio
async def test_missing_terminal_state_returns_none():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.terminal.lookup.{uuid4().hex}"
    group = "test-group"
    event_id = str(uuid4())

    try:
        result = await bus.get_terminal_state(
            stream, group, event_id
        )
        assert result is None
    finally:
        await bus.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state",
    ["committed", "dead_lettered"],
)
async def test_terminal_state_lookup(state):
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.terminal.lookup.{uuid4().hex}"
    group = "test-group"
    event_id = str(uuid4())

    key = f"oaf:terminal:{stream}:{group}:{event_id}"

    try:
        await bus.redis.set(key, state, ex=60)

        result = await bus.get_terminal_state(
            stream, group, event_id
        )

        assert result == state

    finally:
        await bus.redis.delete(key)
        await bus.close()


@pytest.mark.asyncio
async def test_unknown_terminal_state_raises():
    bus = RedisEventBus(REDIS_URL)
    stream = f"oaf.test.terminal.lookup.{uuid4().hex}"
    group = "test-group"
    event_id = str(uuid4())

    key = f"oaf:terminal:{stream}:{group}:{event_id}"

    try:
        await bus.redis.set(key, "unexpected", ex=60)

        with pytest.raises(
            ValueError,
            match="Unexpected terminal state",
        ):
            await bus.get_terminal_state(
                stream, group, event_id
            )

    finally:
        await bus.redis.delete(key)
        await bus.close()
