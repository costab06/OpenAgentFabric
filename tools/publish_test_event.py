import asyncio

from shared.eventbus.events import DocumentDiscoveredEvent, DocumentDiscoveredPayload
from shared.eventbus.redis import RedisEventBus
from shared.models import AgentName


STREAM_NAME = "oaf.events"


async def main() -> None:
    bus = RedisEventBus(redis_url="redis://localhost:6380/0")

    event = DocumentDiscoveredEvent(
        source_agent=AgentName.WATCHER,
        payload=DocumentDiscoveredPayload(
            cik="0000320193",
            accession_number="0000320193-24-000123",
            company_name="Apple Inc.",
            filing_type="10-Q",
            filing_date="2024-08-02",
            filing_url="https://www.sec.gov/example",
        ),
    )

    message_id = await bus.publish(STREAM_NAME, event)
    await bus.close()

    print(f"Published test event: {message_id}")


if __name__ == "__main__":
    asyncio.run(main())
    