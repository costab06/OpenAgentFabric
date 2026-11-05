# OpenAgentFabric Workflow Reliability — foundation patch

Adds `shared.workflow.WorkflowIdentity` and `shared.workflow.StepRegistry`.

## Semantics
- Identity is stable for `(namespace, trigger_type, business_key)`; applications define the business key.
- Step identity is `(correlation_id, agent, step)`; fingerprint mismatch is rejected.
- `claim()` atomically obtains a lease or reports `busy` / `completed`.
- Claims have unique fencing tokens; stale owners cannot complete a reclaimed step.
- Completed step records have no TTL and can be read with `get()`.
- This does **not** yet integrate step completion with `RedisEventBus.commit`.
  Do not use it to skip handler execution until the eventbus terminal marker and
  downstream event publication have been reconciled atomically.
- Lease expiry does not stop the original worker; application side effects must
  be fenced/idempotent. This registry is not a distributed transaction manager.
- The existing CorrelationRegistry has a 24h TTL; durable replay after expiration
  needs a separately designed lifecycle and must not rely on this patch alone.

## Verify
`python -m pytest -q tests/test_workflow_identity.py`

## Next integration step
Extend the existing atomic Redis eventbus commit Lua operation to finalize
step state and publish downstream events together, with ownership checks.
Then integrate BaseAgent and test kill/reclaim/replay against isolated Redis.
