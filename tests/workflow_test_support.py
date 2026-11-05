"""Workflow-aware test utilities for mandatory fenced commits."""
from unittest.mock import AsyncMock
from shared.workflow.registry import StepRegistry, StepCommit, StepClaim

async def claimed_step(bus, source, agent="test-commit", step=None):
    """Create a genuine Redis claim before calling the mandatory commit API."""
    step = step or str(source.event_id)
    fingerprint = "test-input:" + str(source.event_id)
    claim = await StepRegistry(bus.redis).claim(source.correlation_id, agent, step, fingerprint, lease_ms=60000)
    assert claim.status == "claimed", claim.status
    return StepCommit(source.correlation_id, agent, step, fingerprint, claim.token, {"event_id": str(source.event_id)})

def mocked_steps(agent):
    """Unit-test boundary: mock workflow registry, not an incomplete Redis client."""
    steps = AsyncMock()
    steps.claim.return_value = StepClaim("claimed", "test-fencing-token")
    steps.renew.return_value = True
    steps.release.return_value = True
    agent.step_registry = steps
    return steps
