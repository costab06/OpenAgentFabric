from uuid import UUID
import pytest
from shared.workflow import WorkflowIdentity, StepRegistry


def test_identity_stable_and_scoped():
    a = WorkflowIdentity.derive('factor.cycle.requested', 'daily:2026-10-21')
    assert isinstance(a, UUID)
    assert a == WorkflowIdentity.derive('factor.cycle.requested', 'daily:2026-10-21')
    assert a != WorkflowIdentity.derive('factor.cycle.requested', 'daily:2026-10-22')
    assert a != WorkflowIdentity.derive('factor.cycle.requested', 'daily:2026-10-21', namespace='other')


def test_reject_invalid_identity_and_step():
    with pytest.raises(ValueError):
        WorkflowIdentity.derive('x', '')
    with pytest.raises(ValueError):
        StepRegistry.key(WorkflowIdentity.derive('x','y'), 'a:b', 'step')
