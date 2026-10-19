import hashlib
import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field
from redis.asyncio import Redis


class CorrelationRecord(BaseModel):
    correlation_id: UUID
    trigger_type: str
    trigger_timestamp: datetime
    business_fields: dict[str, Any]
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class CorrelationRegistry:
    PREFIX = "oaf:correlation:"
    DEFAULT_TTL = 86400  # 24 hours

    def __init__(
        self,
        redis: Redis,
        ttl_seconds: int = DEFAULT_TTL,
    ):
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")

        self.redis = redis
        self.ttl_seconds = ttl_seconds

    @staticmethod
    def derive_id(
        trigger_type: str,
        trigger_timestamp: datetime,
        business_fields: dict[str, Any],
    ) -> UUID:
        if trigger_timestamp.tzinfo is None:
            raise ValueError("trigger_timestamp must be timezone-aware")

        canonical = {
            "trigger_type": trigger_type,
            "trigger_timestamp": trigger_timestamp.astimezone(
                timezone.utc
            ).isoformat(),
            "business_fields": business_fields,
        }

        encoded = json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")

        digest = hashlib.sha256(encoded).digest()

        return UUID(bytes=digest[:16])

    def _key(self, correlation_id: UUID) -> str:
        return f"{self.PREFIX}{correlation_id}"

    async def create(
        self,
        trigger_type: str,
        trigger_timestamp: datetime,
        business_fields: dict[str, Any],
    ) -> CorrelationRecord:
        correlation_id = self.derive_id(
            trigger_type,
            trigger_timestamp,
            business_fields,
        )

        record = CorrelationRecord(
            correlation_id=correlation_id,
            trigger_type=trigger_type,
            trigger_timestamp=trigger_timestamp,
            business_fields=business_fields,
        )

        # NX prevents overwriting an existing record.
        await self.redis.set(
            self._key(correlation_id),
            record.model_dump_json(),
            ex=self.ttl_seconds,
            nx=True,
        )

        stored = await self.get(correlation_id)

        if stored is None:
            raise RuntimeError(
                "Correlation record disappeared after creation"
            )

        return stored

    async def get(
        self,
        correlation_id: UUID,
    ) -> CorrelationRecord | None:
        value = await self.redis.get(self._key(correlation_id))

        if value is None:
            return None

        return CorrelationRecord.model_validate_json(value)

    async def refresh(self, correlation_id: UUID) -> bool:
        return bool(
            await self.redis.expire(
                self._key(correlation_id),
                self.ttl_seconds,
                xx=True,
            )
        )

    async def exists(self, correlation_id: UUID) -> bool:
        return bool(
            await self.redis.exists(self._key(correlation_id))
        )

    async def delete(self, correlation_id: UUID) -> bool:
        return bool(
            await self.redis.delete(self._key(correlation_id))
        )
    
