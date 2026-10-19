from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AgentName(StrEnum):
    SCHEDULER = "scheduler"
    WATCHER = "watcher"
    FETCHER = "fetcher"
    PARSER = "parser"
    CHUNKER = "chunker"
    EMBEDDER = "embedder"
    RETRIEVER = "retriever"
    SYNTHESIZER = "synthesizer"
    EVALUATOR = "evaluator"


class ArtifactType(StrEnum):
    RAW_DOCUMENT = "raw_document"
    PARSED_DOCUMENT = "parsed_document"
    CHUNK_SET = "chunk_set"
    EMBEDDING_SET = "embedding_set"
    ANSWER = "answer"
    EVALUATION = "evaluation"


class PipelineEventType(StrEnum):
    DOCUMENT_DISCOVERED = "document_discovered"
    DOCUMENT_FETCHED = "document_fetched"
    DOCUMENT_PARSED = "document_parsed"
    CHUNKS_CREATED = "chunks_created"
    EMBEDDINGS_CREATED = "embeddings_created"
    INDEX_UPDATED = "index_updated"
    RETRIEVAL_REQUESTED = "retrieval_requested"
    ANSWER_GENERATED = "answer_generated"
    EVALUATION_COMPLETE = "evaluation_complete"


class ArtifactRef(BaseModel):
    artifact_id: str
    artifact_type: ArtifactType
    uri: str
    version: int = 1
    checksum: str | None = None
    created_at: datetime = Field(default_factory=utc_now)


class PipelineEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: PipelineEventType
    source_agent: AgentName
    correlation_id: UUID = Field(default_factory=uuid4)
    artifact: ArtifactRef | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)


class A2ARequest(BaseModel):
    request_id: UUID = Field(default_factory=uuid4)
    source_agent: AgentName
    target_agent: AgentName
    correlation_id: UUID
    event: PipelineEvent
    created_at: datetime = Field(default_factory=utc_now)


class A2AResponse(BaseModel):
    request_id: UUID
    accepted: bool
    message: str
    created_at: datetime = Field(default_factory=utc_now)
    