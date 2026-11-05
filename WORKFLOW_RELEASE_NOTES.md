# OAF workflow runtime candidate (breaking change)

Mandatory fenced step claim and atomic completion for BaseAgent messages. Steps are keyed by (correlation ID, agent name, event ID), and fingerprints cover the serialized event. Completed steps are ACKed without re-running handlers. Active step leases renew while handlers execute. Correlation records no longer expire.

**Not yet production complete:** This is an integration candidate, not the complete release. Durable workflow orchestration, deterministic trigger publication, handling distinct duplicate event IDs for one logical business step, external side-effect contracts, and comprehensive restart/failure tests remain. Existing tests/mocks may require migration to the new required step context.
