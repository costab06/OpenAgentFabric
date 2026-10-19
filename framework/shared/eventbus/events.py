from datetime import datetime, timezone
from enum import StrEnum
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from shared.models import AgentName, ArtifactRef


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class EventType(StrEnum):
    DOCUMENT_DISCOVERED = "document.discovered"
    DOCUMENT_FETCHED = "document.fetched"
    DOCUMENT_PARSED = "document.parsed"
    CHUNKS_CREATED = "chunks.created"
    EMBEDDINGS_CREATED = "embeddings.created"
    INDEX_UPDATED = "index.updated"
    RETRIEVAL_REQUESTED = "retrieval.requested"
    ANSWER_GENERATED = "answer.generated"
    EVALUATION_COMPLETE = "evaluation.complete"


class EventMetadata(BaseModel):
    schema_version: str = "1.0"
    environment: str = "dev"
    tags: dict[str, str] = Field(default_factory=dict)


class DocumentDiscoveredPayload(BaseModel):
    cik: str
    accession_number: str
    company_name: str
    filing_type: str
    filing_date: str
    filing_url: str


class DocumentFetchedPayload(BaseModel):
    cik: str
    accession_number: str
    company_name: str
    filing_type: str
    artifact: ArtifactRef


class DocumentParsedPayload(BaseModel):
    source_artifact: ArtifactRef
    parsed_artifact: ArtifactRef


class ChunksCreatedPayload(BaseModel):
    source_artifact: ArtifactRef
    chunk_artifact: ArtifactRef
    chunk_count: int


class EventEnvelope(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    source_agent: AgentName

    correlation_id: UUID = Field(default_factory=uuid4)
    causation_id: UUID | None = None

    created_at: datetime = Field(default_factory=utc_now)
    metadata: EventMetadata = Field(default_factory=EventMetadata)

    payload: (
        DocumentDiscoveredPayload
        | DocumentFetchedPayload
        | DocumentParsedPayload
        | ChunksCreatedPayload
    )


class DocumentDiscoveredEvent(EventEnvelope):
    event_type: Literal[EventType.DOCUMENT_DISCOVERED] = EventType.DOCUMENT_DISCOVERED
    payload: DocumentDiscoveredPayload


class DocumentFetchedEvent(EventEnvelope):
    event_type: Literal[EventType.DOCUMENT_FETCHED] = EventType.DOCUMENT_FETCHED
    payload: DocumentFetchedPayload


class DocumentParsedEvent(EventEnvelope):
    event_type: Literal[EventType.DOCUMENT_PARSED] = EventType.DOCUMENT_PARSED
    payload: DocumentParsedPayload


class ChunksCreatedEvent(EventEnvelope):
    event_type: Literal[EventType.CHUNKS_CREATED] = EventType.CHUNKS_CREATED
    payload: ChunksCreatedPayload
    