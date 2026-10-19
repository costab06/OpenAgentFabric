# ADR 0002: Events as Primary Abstraction with Redis Streams Transport

## Status

Accepted

## Context

The system is a distributed multi-agent RAG pipeline. Each stage is implemented as an independent LangGraph application. Agents should remain loosely coupled and should not depend directly on the internal implementation of other agents.

The system needs:

- asynchronous processing
- retry support
- replayability
- auditability
- correlation IDs
- eventual support for distributed deployment
- clear separation between business events and transport

## Decision

Events are the primary architectural abstraction.

Agents do not primarily call one another directly to move the pipeline forward. Instead, each agent:

1. consumes domain events
2. performs its assigned work
3. writes artifacts or metadata
4. publishes new domain events

Redis Streams will be the initial event transport.

A2A remains part of the architecture, but it is used for governed agent-to-agent command/API interactions rather than as the primary event backbone.

## Consequences

### Benefits

- Agents are loosely coupled.
- Events are durable.
- Events can be replayed.
- Failed work can be retried.
- The pipeline can be observed through the event stream.
- Additional agents can subscribe to existing events.
- Redis Streams can later be replaced by Kafka or another event platform behind the same abstraction.

### Tradeoffs

- The system requires an event bus dependency.
- Event schema design becomes important.
- Agents must be idempotent.
- Operational monitoring for Redis is required.

## Initial Implementation

The first implementation will include:

- a shared event bus abstraction
- a Redis Streams implementation
- common event envelope schema
- consumer group support
- correlation IDs
- tests using either a local Redis instance or mocked Redis client

## Relationship to A2A

Redis Streams moves pipeline events.

A2A exposes governed agent capabilities.

Manetu or a similar governance layer can gate A2A calls, while the event bus provides the asynchronous processing backbone.

