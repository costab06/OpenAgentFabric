
from collections.abc import AsyncIterator, Sequence
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from shared.eventbus.base import EventBus
from shared.eventbus.events import EventEnvelope
from datetime import datetime, timezone
from shared.eventbus.pending import PendingMessage



ATOMIC_COMMIT_SCRIPT = """
local stream = KEYS[1]
local terminal_key = KEYS[2]
local attempt_key = KEYS[3]

local group = ARGV[1]
local message_id = ARGV[2]
local expected_event_id = ARGV[3]
local ttl_seconds = tonumber(ARGV[4])
local event_count = tonumber(ARGV[5])

-- Downstream events occupy ARGV[6] through ARGV[5 + event_count].
-- The optional consumer follows the downstream events.
local expected_consumer = ARGV[6 + event_count] or ''

-- Enforce mutually exclusive terminal states.
local terminal_state = redis.call('GET', terminal_key)

if terminal_state == 'committed' then
    return 0
end

if terminal_state == 'dead_lettered' then
    return redis.error_reply('OAF_ALREADY_DEAD_LETTERED')
end

-- Verify that the source message exists.
local source = redis.call(
    'XRANGE', stream, message_id, message_id
)

if #source ~= 1 then
    return redis.error_reply('OAF_SOURCE_NOT_FOUND')
end

-- Verify that the source is pending in this consumer group.
local pending = redis.call(
    'XPENDING', stream, group, message_id, message_id, 1
)

if #pending ~= 1 then
    return redis.error_reply('OAF_SOURCE_NOT_PENDING')
end

-- Verify ownership when a consumer name is supplied.
if expected_consumer ~= '' and pending[1][2] ~= expected_consumer then
    return redis.error_reply('OAF_CONSUMER_OWNERSHIP_MISMATCH')
end

-- Extract the serialized source event.
local fields = source[1][2]
local serialized_source = nil

for i = 1, #fields, 2 do
    if fields[i] == 'event' then
        serialized_source = fields[i + 1]
        break
    end
end

if not serialized_source then
    return redis.error_reply('OAF_SOURCE_EVENT_MISSING')
end

-- Validate source JSON and event identity.
local ok, decoded = pcall(cjson.decode, serialized_source)

if not ok or type(decoded) ~= 'table' then
    return redis.error_reply('OAF_INVALID_SOURCE_JSON')
end

if decoded.event_id ~= expected_event_id then
    return redis.error_reply('OAF_SOURCE_EVENT_ID_MISMATCH')
end

-- Validate every downstream event before performing writes.
for i = 1, event_count do
    local payload = ARGV[5 + i]
    local valid, downstream = pcall(cjson.decode, payload)

    if not valid or type(downstream) ~= 'table' then
        return redis.error_reply('OAF_INVALID_DOWNSTREAM_JSON')
    end

    if downstream.correlation_id ~= decoded.correlation_id then
        return redis.error_reply('OAF_CORRELATION_ID_MISMATCH')
    end

    if downstream.causation_id ~= decoded.event_id then
        return redis.error_reply('OAF_CAUSATION_ID_MISMATCH')
    end
end

-- Publish downstream events.
for i = 1, event_count do
    redis.call(
        'XADD', stream, '*', 'event', ARGV[5 + i]
    )
end

-- Acknowledge the source event.
redis.call('XACK', stream, group, message_id)
redis.call('SET', terminal_key, 'committed', 'EX', ttl_seconds)
redis.call('DEL', attempt_key)
return 1
"""



ATOMIC_DEAD_LETTER_SCRIPT = """
local source_stream = KEYS[1]
local dlq_stream = KEYS[2]
local terminal_key = KEYS[3]
local attempt_key = KEYS[4]

local group = ARGV[1]
local message_id = ARGV[2]
local expected_event_id = ARGV[3]
local error_message = ARGV[4]
local attempts = tonumber(ARGV[5])
local agent = ARGV[6]
local timestamp = ARGV[7]
local ttl_seconds = tonumber(ARGV[8])

-- Optional consumer is the final argument.
local expected_consumer = ARGV[9] or ''

-- Enforce mutually exclusive terminal states.
local terminal_state = redis.call('GET', terminal_key)

if terminal_state == 'dead_lettered' then
    return 0
end

if terminal_state == 'committed' then
    return redis.error_reply('OAF_ALREADY_COMMITTED')
end

-- Verify that the source message exists.
local source = redis.call(
    'XRANGE', source_stream, message_id, message_id
)

if #source ~= 1 then
    return redis.error_reply('OAF_SOURCE_NOT_FOUND')
end

-- Verify that the source is pending in this consumer group.
local pending = redis.call(
    'XPENDING', source_stream, group, message_id, message_id, 1
)

if #pending ~= 1 then
    return redis.error_reply('OAF_SOURCE_NOT_PENDING')
end

-- Verify ownership when a consumer name is supplied.
if expected_consumer ~= '' and pending[1][2] ~= expected_consumer then
    return redis.error_reply('OAF_CONSUMER_OWNERSHIP_MISMATCH')
end

-- Extract the serialized source event.
local fields = source[1][2]
local serialized_event = nil

for i = 1, #fields, 2 do
    if fields[i] == 'event' then
        serialized_event = fields[i + 1]
        break
    end
end

if not serialized_event then
    return redis.error_reply('OAF_SOURCE_EVENT_MISSING')
end

-- Validate source JSON and event identity.
local ok, decoded = pcall(cjson.decode, serialized_event)

if not ok or type(decoded) ~= 'table' then
    return redis.error_reply('OAF_INVALID_SOURCE_JSON')
end

if decoded.event_id ~= expected_event_id then
    return redis.error_reply('OAF_SOURCE_EVENT_ID_MISMATCH')
end

-- Write the original event and failure metadata to the DLQ.
redis.call(
    'XADD', dlq_stream, '*',
    'event', serialized_event,
    'source_stream', source_stream,
    'source_group', group,
    'source_message_id', message_id,
    'error', error_message,
    'attempts', attempts,
    'agent', agent,
    'failed_at', timestamp
)

-- Acknowledge the original event.
redis.call('XACK', source_stream, group, message_id)
redis.call('SET', terminal_key, 'dead_lettered', 'EX', ttl_seconds)
redis.call('DEL', attempt_key)
return 1
"""



BEGIN_ATTEMPT_SCRIPT = """
local stream = KEYS[1]
local attempt_key = KEYS[2]

local group = ARGV[1]
local message_id = ARGV[2]
local expected_consumer = ARGV[3]
local ttl_seconds = tonumber(ARGV[4])

-- The message must still be pending.
local pending = redis.call(
    'XPENDING', stream, group, message_id, message_id, 1
)

if #pending ~= 1 then
    return redis.error_reply('OAF_SOURCE_NOT_PENDING')
end

-- Only the current owner may start a processing attempt.
if pending[1][2] ~= expected_consumer then
    return redis.error_reply('OAF_CONSUMER_OWNERSHIP_MISMATCH')
end

-- Increment the count atomically.
local attempts = redis.call('INCR', attempt_key)

-- Refresh the attempt-state retention period.
redis.call('EXPIRE', attempt_key, ttl_seconds)

return attempts
"""


HEARTBEAT_SCRIPT = """
local stream = KEYS[1]

local group = ARGV[1]
local message_id = ARGV[2]
local consumer = ARGV[3]

local pending = redis.call(
    'XPENDING', stream, group, message_id, message_id, 1
)

if #pending ~= 1 then
    return 0
end

if pending[1][2] ~= consumer then
    return 0
end

-- Refresh the idle timer without changing ownership.
-- JUSTID prevents incrementing the delivery counter.
local claimed = redis.call(
    'XCLAIM',
    stream,
    group,
    consumer,
    0,
    message_id,
    'JUSTID'
)

if #claimed ~= 1 then
    return 0
end

return 1
"""


class RedisEventBus(EventBus):
    """
    Redis Streams implementation of the OpenAgentFabric event bus.

    Features:
      - Typed event publication
      - Consumer-group subscriptions
      - Message acknowledgement
      - Pending-message recovery
      - Atomic downstream publication and acknowledgement
      - Duplicate commit protection
    """

    DEFAULT_REDIS_URL = "redis://localhost:6379/0"
    DEFAULT_COMMIT_TTL = 86400

    def __init__(
        self,
        redis_url: str = DEFAULT_REDIS_URL,
    ):
        self.redis = Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_timeout=10,
            socket_connect_timeout=5,
            health_check_interval=30,
        )

    async def publish(
        self,
        stream: str,
        event: EventEnvelope,
    ) -> str:
        """
        Publish an event to a Redis stream.

        Returns the Redis message ID.
        """
        message_id = await self.redis.xadd(
            stream,
            {"event": event.model_dump_json()},
        )

        return str(message_id)

    async def subscribe(
        self,
        stream: str,
        group: str,
        consumer: str,
        count: int = 10,
        block_ms: int = 5000,
    ) -> AsyncIterator[tuple[str, EventEnvelope]]:
        """
        Continuously consume new events from a Redis consumer group.

        Each agent type should use a distinct group.
        Multiple instances of the same agent can share a group.

        This method intentionally runs until cancelled or until the
        caller stops iterating.
        """
        await self._ensure_group(stream, group)

        while True:
            messages = await self.redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=count,
                block=block_ms,
            )

            if not messages:
                continue

            for _, stream_messages in messages:
                for message_id, fields in stream_messages:
                    event = EventEnvelope.model_validate_json(
                        fields["event"]
                    )

                    yield str(message_id), event

    async def acknowledge(
        self,
        stream: str,
        group: str,
        message_id: str,
    ) -> None:
        """
        Acknowledge a successfully processed message.
        """
        await self.redis.xack(
            stream,
            group,
            message_id,
        )


    async def reclaim_pending(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_ms: int = 30_000,
        count: int = 100,
    ) -> list[tuple[str, EventEnvelope]]:
        """
        Compatibility method: reclaim one batch from the beginning.
        """
        _, messages = await self.reclaim_pending_batch(
            stream=stream,
            group=group,
            consumer=consumer,
            cursor="0-0",
            min_idle_ms=min_idle_ms,
            count=count,
        )
        return messages


    async def reclaim_pending_batch(
        self,
        stream: str,
        group: str,
        consumer: str,
        cursor: str = "0-0",
        min_idle_ms: int = 30_000,
        count: int = 100,
    ) -> tuple[str, list[tuple[str, EventEnvelope]]]:
        """
        Reclaim one batch of abandoned messages.

        Returns:
            (next_cursor, recovered_messages)

        A next_cursor of "0-0" indicates the end of the scan.
        """
        await self._ensure_group(stream, group)

        result = await self.redis.xautoclaim(
            name=stream,
            groupname=group,
            consumername=consumer,
            min_idle_time=min_idle_ms,
            start_id=cursor,
            count=count,
        )

        next_cursor = str(result[0])
        messages = result[1]

        recovered = [
            (
                str(message_id),
                EventEnvelope.model_validate_json(fields["event"]),
            )
            for message_id, fields in messages
        ]

        return next_cursor, recovered

    async def commit(
            self,
            stream: str,
            group: str,
            message_id: str,
            source_event_id: UUID,
            downstream_events: Sequence[EventEnvelope],
            consumer: str | None = None,
    ) -> bool:
        """
        Validate and atomically commit an event-processing result.

        Returns True for a new commit and False for a duplicate.

        Raises redis.exceptions.ResponseError if the source
        message or downstream events fail validation.
        """
        terminal_key = (
            f"oaf:terminal:{stream}:{group}:{source_event_id}"
        )

        serialized_events = [
            event.model_dump_json()
            for event in downstream_events
        ]

        result = await self.redis.eval(
            ATOMIC_COMMIT_SCRIPT,
            3,
            stream,
            terminal_key,
            self._attempt_key(stream, group, message_id),
            group,
            message_id,
            str(source_event_id),
            self.DEFAULT_COMMIT_TTL,
            len(serialized_events),
            *serialized_events,
            consumer or "",
        )

        return bool(result)


        serialized_events = [
            event.model_dump_json()
            for event in downstream_events
        ]

        result = await self.redis.eval(
            ATOMIC_COMMIT_SCRIPT,
            2,
            stream,
            completion_key,
            group,
            message_id,
            self.DEFAULT_COMMIT_TTL,
            len(serialized_events),
            *serialized_events,
        )

        return bool(result)

    async def dead_letter(
            self,
            stream: str,
            group: str,
            message_id: str,
            source_event_id: UUID,
            error: str,
            attempts: int,
            agent: str,
            dlq_stream: str = "oaf.events.dlq",
            consumer: str | None = None,
    ) -> bool:
        """
        Atomically publish a failure record to the DLQ
        and acknowledge the original pending message.
        """
        if attempts < 1:
            raise ValueError("attempts must be positive")

        terminal_key = (
            f"oaf:terminal:{stream}:{group}:{source_event_id}"
        )

        timestamp = datetime.now(timezone.utc).isoformat()

        result = await self.redis.eval(
            ATOMIC_DEAD_LETTER_SCRIPT,
            4,
            stream,
            dlq_stream,
            terminal_key,
            self._attempt_key(stream, group, message_id),
            group,
            message_id,
            str(source_event_id),
            error,
            attempts,
            agent,
            datetime.now(timezone.utc).isoformat(),
            self.DEFAULT_COMMIT_TTL,
            consumer or "",
        )

        return bool(result)

    @staticmethod
    def _attempt_key(
        stream: str,
        group: str,
        message_id: str,
    ) -> str:
        return f"oaf:attempts:{stream}:{group}:{message_id}"

    async def begin_attempt(
        self,
        stream: str,
        group: str,
        message_id: str,
        consumer: str,
    ) -> int:
        if not consumer:
            raise ValueError("consumer must not be empty")

        attempt_key = self._attempt_key(
            stream, group, message_id
        )

        result = await self.redis.eval(
            BEGIN_ATTEMPT_SCRIPT,
            2,
            stream,
            attempt_key,
            group,
            message_id,
            consumer,
            self.DEFAULT_COMMIT_TTL,
        )

        return int(result)

    async def get_attempt_count(
        self,
        stream: str,
        group: str,
        message_id: str,
    ) -> int:
        attempt_key = self._attempt_key(
            stream, group, message_id
        )

        value = await self.redis.get(attempt_key)
        return int(value) if value is not None else 0

    async def clear_attempts(
        self,
        stream: str,
        group: str,
        message_id: str,
    ) -> None:
        attempt_key = self._attempt_key(
            stream, group, message_id
        )

        await self.redis.delete(attempt_key)

    async def get_pending_message(
        self,
        stream: str,
        group: str,
        message_id: str,
    ) -> PendingMessage | None:
        """
        Retrieve one pending-message entry using XPENDING.

        Redis returns:
          message_id
          consumer
          time_since_delivered
          times_delivered
        """
        entries = await self.redis.xpending_range(
            name=stream,
            groupname=group,
            min=message_id,
            max=message_id,
            count=1,
        )

        if not entries:
            return None

        entry = entries[0]

        if str(entry["message_id"]) != message_id:
            return None

        return PendingMessage(
            message_id=str(entry["message_id"]),
            consumer=str(entry["consumer"]),
            delivery_count=int(entry["times_delivered"]),
            idle_ms=int(entry["time_since_delivered"]),
        )

    async def heartbeat(
        self,
        stream: str,
        group: str,
        message_id: str,
        consumer: str,
    ) -> bool:
        if not consumer:
            raise ValueError("consumer must not be empty")

        result = await self.redis.eval(
            HEARTBEAT_SCRIPT,
            1,
            stream,
            group,
            message_id,
            consumer,
        )

        return bool(result)

    async def health(self) -> bool:
        """
        Check whether Redis is reachable.
        """
        try:
            return bool(await self.redis.ping())
        except Exception:
            return False

    async def close(self) -> None:
        """
        Close the Redis connection pool.
        """
        await self.redis.aclose()

    async def get_terminal_state(
        self,
        stream: str,
        group: str,
        source_event_id: str,
    ) -> str | None:
        key = (
            f"oaf:terminal:{stream}:{group}:"
            f"{source_event_id}"
        )

        value = await self.redis.get(key)

        if value is None:
            return None

        if isinstance(value, bytes):
            value = value.decode("utf-8")

        if value not in ("committed", "dead_lettered"):
            raise ValueError(
                f"Unexpected terminal state {value!r} "
                f"for {key}"
            )

        return value

    async def _ensure_group(
        self,
        stream: str,
        group: str,
    ) -> None:
        """
        Create a consumer group if it does not already exist.

        Starting at ID 0 ensures that a newly created group can
        consume events already present in the stream.
        """
        try:
            await self.redis.xgroup_create(
                name=stream,
                groupname=group,
                id="0",
                mkstream=True,
            )

        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
