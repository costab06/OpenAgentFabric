"""Atomic Redis-backed step claims with fencing tokens and durable completion.

This registry deliberately does NOT mark eventbus messages committed. Applications
must reconcile step results with OpenAgentFabric's atomic downstream event commit.
"""
from __future__ import annotations
from dataclasses import dataclass
from uuid import UUID, uuid4
import json
from typing import Any
from redis.asyncio import Redis

CLAIM_LUA = """
local key = KEYS[1]
local fingerprint, token, now, lease = ARGV[1], ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4])
local prior = redis.call('HGET', key, 'fingerprint')
if prior and prior ~= fingerprint then return {'conflict', ''} end
local status = redis.call('HGET', key, 'status')
if status == 'completed' then return {'completed', ''} end
local until_ms = tonumber(redis.call('HGET', key, 'lease_until') or '0')
if status == 'running' and until_ms > now then return {'busy', ''} end
redis.call('HSET', key, 'fingerprint', fingerprint, 'status', 'running', 'token', token, 'lease_until', now + lease)
return {'claimed', token}
"""
COMPLETE_LUA = """
local key = KEYS[1]
local token, fingerprint, result = ARGV[1], ARGV[2], ARGV[3]
if redis.call('HGET', key, 'fingerprint') ~= fingerprint then return 'conflict' end
if redis.call('HGET', key, 'status') == 'completed' then return 'completed' end
if redis.call('HGET', key, 'status') ~= 'running' or redis.call('HGET', key, 'token') ~= token then return 'lost' end
redis.call('HSET', key, 'status', 'completed', 'result', result)
redis.call('HDEL', key, 'lease_until', 'token')
return 'completed'
"""

class StepConflict(ValueError):
    pass

@dataclass(frozen=True)
class StepClaim:
    status: str  # claimed, busy, completed
    token: str | None = None


@dataclass(frozen=True)
class StepCommit:
    """A fenced step completion to atomically include in EventBus.commit.

    The caller must hold a live claim and renew its lease during long work.
    """
    correlation_id: UUID
    agent: str
    step: str
    fingerprint: str
    token: str
    result: dict[str, Any]

    def encoded_result(self) -> str:
        if not self.fingerprint or not self.token:
            raise ValueError("Step fingerprint and claim token are required")
        return json.dumps(self.result, sort_keys=True, separators=(',', ':'), allow_nan=False)


RELEASE_LUA = """
local key = KEYS[1]
local fingerprint, token = ARGV[1], ARGV[2]
if redis.call('HGET', key, 'fingerprint') ~= fingerprint or
   redis.call('HGET', key, 'status') ~= 'running' or
   redis.call('HGET', key, 'token') ~= token then return 0 end
redis.call('HSET', key, 'status', 'released')
redis.call('HDEL', key, 'token', 'lease_until')
return 1
"""

RENEW_LUA = """
local key = KEYS[1]
local fingerprint, token, lease = ARGV[1], ARGV[2], tonumber(ARGV[3])
if redis.call('HGET', key, 'fingerprint') ~= fingerprint or
   redis.call('HGET', key, 'status') ~= 'running' or
   redis.call('HGET', key, 'token') ~= token then return 0 end
local time = redis.call('TIME')
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if tonumber(redis.call('HGET', key, 'lease_until') or '0') <= now_ms then return 0 end
redis.call('HSET', key, 'lease_until', now_ms + lease)
return 1
"""

class StepRegistry:
    PREFIX = 'oaf:workflow:step:'

    def __init__(self, redis: Redis):
        self.redis = redis

    @classmethod
    def key(cls, correlation_id: UUID, agent: str, step: str) -> str:
        if not agent or not step or ':' in agent or ':' in step:
            raise ValueError('agent and step must be nonempty and must not contain colons')
        return f'{cls.PREFIX}{correlation_id}:{agent}:{step}'

    async def claim(self, correlation_id: UUID, agent: str, step: str,
                    fingerprint: str, *, lease_ms: int = 30000) -> StepClaim:
        if not fingerprint or lease_ms <= 0:
            raise ValueError('fingerprint must be nonempty and lease_ms positive')
        # Redis TIME avoids host-clock disagreement between workers.
        seconds, micros = await self.redis.time()
        now_ms = int(seconds) * 1000 + int(micros) // 1000
        token = str(uuid4())
        status, granted = await self.redis.eval(CLAIM_LUA, 1, self.key(correlation_id, agent, step),
                                                fingerprint, token, now_ms, lease_ms)
        if status == 'conflict':
            raise StepConflict('Step identity reused with a different input fingerprint')
        return StepClaim(status, granted or None)

    async def renew(self, correlation_id: UUID, agent: str, step: str,
                    fingerprint: str, token: str, *, lease_ms: int = 30000) -> bool:
        """Renew an unexpired claim; never resurrect a stale token."""
        if not fingerprint or not token or lease_ms <= 0:
            raise ValueError("Valid fingerprint, token and positive lease required")
        return bool(await self.redis.eval(RENEW_LUA, 1,
            self.key(correlation_id, agent, step), fingerprint, token, lease_ms))

    async def release(self, correlation_id: UUID, agent: str, step: str,
                      fingerprint: str, token: str) -> bool:
        """Release only a claim still owned by this token; never erase completion."""
        if not fingerprint or not token:
            raise ValueError("Fingerprint and claim token required")
        return bool(await self.redis.eval(RELEASE_LUA, 1,
            self.key(correlation_id, agent, step), fingerprint, token))

    async def complete(self, correlation_id: UUID, agent: str, step: str,
                       fingerprint: str, token: str, result: dict[str, Any]) -> None:
        if not token:
            raise ValueError('A valid claim token is required')
        encoded = json.dumps(result, sort_keys=True, separators=(',', ':'), allow_nan=False)
        status = await self.redis.eval(COMPLETE_LUA, 1, self.key(correlation_id, agent, step),
                                       token, fingerprint, encoded)
        if status != 'completed':
            raise StepConflict(f'Cannot complete step: {status}')

    async def get(self, correlation_id: UUID, agent: str, step: str) -> dict[str, Any] | None:
        values = await self.redis.hgetall(self.key(correlation_id, agent, step))
        if not values:
            return None
        def decode(value):
            return value.decode() if isinstance(value, bytes) else value
        record = {decode(k): decode(v) for k, v in values.items()}
        if 'result' in record:
            record['result'] = json.loads(record['result'])
        return record
