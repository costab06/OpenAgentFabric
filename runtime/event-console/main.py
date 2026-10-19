import asyncio
import json
from datetime import datetime
from uuid import UUID

from shared.eventbus.redis import RedisEventBus
from shared.eventbus.events import EventEnvelope


STREAM_NAME = "oaf.events"
GROUP_NAME = "event-console"
CONSUMER_NAME = "event-console-1"


def short_id(value: UUID | None) -> str:
    if value is None:
        return "-"
    return str(value)[:8]


def summarize_payload(event: EventEnvelope) -> str:
    payload = event.payload.model_dump()

    if "company_name" in payload and "filing_type" in payload:
        return f"{payload['company_name']} {payload['filing_type']}"

    if "artifact" in payload:
        artifact = payload["artifact"]
        return f"artifact={artifact.get('artifact_id', '-')}"
    
    return json.dumps(payload, default=str)[:120]


def format_event(message_id: str, event: EventEnvelope) -> str:
    timestamp = event.created_at.isoformat(timespec="seconds")

    return (
        f"\n[{timestamp}] {event.event_type}\n"
        f"  message_id:     {message_id}\n"
        f"  source_agent:   {event.source_agent}\n"
        f"  conversation:   {short_id(event.correlation_id)}\n"
        f"  causation:      {short_id(event.causation_id)}\n"
        f"  event_id:       {short_id(event.event_id)}\n"
        f"  payload:        {summarize_payload(event)}"
    )


async def run() -> None:
    bus = RedisEventBus(redis_url="redis://localhost:6380/0")

    print("OpenAgentFabric Event Console")
    print(f"Listening on Redis stream: {STREAM_NAME}")
    print("Press Ctrl+C to stop")

    try:
        async for message_id, event in bus.subscribe(
            stream=STREAM_NAME,
            group=GROUP_NAME,
            consumer=CONSUMER_NAME,
            count=10,
            block_ms=1000,
        ):
            print(format_event(message_id, event))
            await bus.acknowledge(STREAM_NAME, GROUP_NAME, message_id)

    finally:
        await bus.close()

if __name__ == "__main__":
    asyncio.run(run())