"""Workflow replay must verify the atomic eventbus commit."""
from unittest.mock import AsyncMock
from uuid import uuid4
import pytest
from shared.agent.base import BaseAgent
from shared.eventbus.events import EventEnvelope
from shared.workflow.registry import StepClaim, StepConflict

class Agent(BaseAgent):
    def __init__(self, bus, correlations):
        super().__init__("worker", bus, correlations, {"work"})
        self.calls = 0
    async def handle_event(self, event):
        self.calls += 1
        return []

@pytest.mark.asyncio
@pytest.mark.parametrize("terminal,result,should_raise", [
    ("committed", True, False),
    (None, True, True),
    ("dead_lettered", True, True),
    ("committed", False, True),
])
async def test_completed_step_replay_requires_verified_commit(terminal, result, should_raise):
    bus, correlations = AsyncMock(), AsyncMock()
    bus.redis = AsyncMock()
    bus.get_attempt_count.return_value = 0
    bus.begin_attempt.return_value = 1
    bus.get_terminal_state.return_value = terminal
    agent = Agent(bus, correlations)
    event = EventEnvelope(event_type="work", source_agent="origin", correlation_id=uuid4(), payload={})
    agent.step_registry = AsyncMock()
    agent.step_registry.claim.return_value = StepClaim("completed")
    agent.step_registry.get.return_value = {"result": {"event_id": str(event.event_id) if result else "wrong"}}
    if should_raise:
        with pytest.raises(StepConflict):
            await agent.process_message("1-0", event)
    else:
        await agent.process_message("1-0", event)
    assert agent.calls == 0
    bus.commit.assert_not_awaited()
