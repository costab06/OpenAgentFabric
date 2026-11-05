# Workflow replay safety correction

This patch applies on top of the runtime candidate and retry fix.

- Reject completed-step replay unless its stored result identifies the source event and the eventbus terminal marker is committed.
- Keep committed terminal markers durable, matching non-expiring step completion records.
- Add four unit scenarios for replay guard behavior.

**Scope:** This is a safety correction, not the complete 21-failure test migration or the final OAF release. The current container lacks the Redis Python package and a Redis server; tests have not been executed here. Keep the branch uncommitted.
