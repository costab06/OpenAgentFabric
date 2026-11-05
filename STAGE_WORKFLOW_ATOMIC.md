# Workflow atomic commit — opt-in integration

`RedisEventBus.commit(..., step_commit=StepCommit(...))` atomically validates the live step fence, publishes downstream events, acknowledges the source message, and completes the step. Existing `commit` callers are unchanged.

`StepRegistry.renew(...)` refreshes a live lease. Callers must renew periodically during long-running operations; `BaseAgent` is **not yet wired to StepRegistry**. This patch is the atomic transaction layer, not end-to-end workflow recovery.

Tests use Redis database 14 at localhost:6380 and unique test keys. Verify that database 14 is safe to use before running.

Do not use `StepRegistry.complete()` separately before `commit` for event-driven steps.
