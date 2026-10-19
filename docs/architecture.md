# Architecture

## Design Philosophy

The system follows five architectural principles.

### 1. Distributed by Default

Every logical capability executes as an independent process.

Advantages include:

* independent deployment
* independent scaling
* fault isolation
* policy enforcement
* clear ownership

---

### 2. Communication Through A2A Only

Agents never invoke Python functions belonging to another agent.

All communication occurs through A2A.

This allows:

* authentication
* authorization
* audit logging
* protocol versioning
* deployment independence

---

### 3. Shared Artifact Store

Agents exchange references rather than documents.

Instead of transmitting a 30 MB filing between services:

```
artifact_id:
    sec/nvda/2026/Q2

version:
    4

checksum:
    ...
```

The receiving agent retrieves the document independently.

Benefits include:

* lower network utilization
* restartability
* reproducibility
* immutable processing history

---

### 4. Event-Driven Processing

Each completed stage emits an event.

Example:

```
DOCUMENT_FETCHED

↓

DOCUMENT_PARSED

↓

CHUNKS_CREATED

↓

EMBEDDINGS_COMPLETE

↓

INDEX_UPDATED
```

This allows retries without reprocessing the entire pipeline.

---

### 5. Zero-Trust Security

Every A2A request is authenticated.

Every request is authorized.

Every request is logged.

Every artifact access is auditable.

---

## Processing Pipeline

Scheduler

↓

Watcher

↓

Fetcher

↓

Parser

↓

Chunker

↓

Embedder

↓

Retriever

↓

Synthesizer

↓

Evaluator

---

## Future Enhancements

* Multiple ingestion pipelines
* Human approval agents
* Multi-tenant deployments
* Hybrid cloud/local inference
* Sensitive data classification
* Distributed vector databases
* Policy simulation
* Real-time monitoring

