from datetime import datetime, timezone, timedelta

import pytest
from redis.asyncio import Redis

from shared.correlation.registry import CorrelationRegistry


def test_correlation_id_is_deterministic():
    timestamp = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)

    fields_a = {"cik": "0000320193", "filing": "10-K"}
    fields_b = {"filing": "10-K", "cik": "0000320193"}

    first = CorrelationRegistry.derive_id(
        "sec.filing.discovered", timestamp, fields_a
    )

    second = CorrelationRegistry.derive_id(
        "sec.filing.discovered", timestamp, fields_b
    )

    assert first == second

    later = CorrelationRegistry.derive_id(
        "sec.filing.discovered",
        timestamp + timedelta(seconds=1),
        fields_a,
    )

    assert first != later


def test_naive_timestamp_rejected():
    with pytest.raises(ValueError):
        CorrelationRegistry.derive_id(
            "sec.filing.discovered",
            datetime(2026, 10, 13),
            {"cik": "0000320193"},
        )


@pytest.mark.asyncio
async def test_correlation_registry_lifecycle():
    redis = Redis.from_url(
        "redis://localhost:6380/0",
        decode_responses=True,
    )

    registry = CorrelationRegistry(redis, ttl_seconds=60)

    timestamp = datetime.now(timezone.utc)
    fields = {"cik": "0000320193", "accession": "test-correlation"}

    try:
        record = await registry.create(
            "sec.filing.discovered",
            timestamp,
            fields,
        )

        assert await registry.exists(record.correlation_id)

        retrieved = await registry.get(record.correlation_id)
        assert retrieved is not None
        assert retrieved.correlation_id == record.correlation_id

        duplicate = await registry.create(
            "sec.filing.discovered",
            timestamp,
            fields,
        )

        assert duplicate.correlation_id == record.correlation_id
        assert duplicate.created_at == record.created_at

        assert await registry.refresh(record.correlation_id)

        assert await registry.delete(record.correlation_id)
        assert not await registry.exists(record.correlation_id)

        # Refresh must not recreate a deleted record.
        assert not await registry.refresh(record.correlation_id)

    finally:
        await redis.aclose()
        
