from shared.eventbus.events import (
    DocumentDiscoveredEvent,
    DocumentDiscoveredPayload,
    EventType,
)
from shared.models import AgentName


def test_document_discovered_event_has_defaults():
    event = DocumentDiscoveredEvent(
        source_agent=AgentName.WATCHER,
        payload=DocumentDiscoveredPayload(
            cik="0000320193",
            accession_number="0000320193-24-000123",
            company_name="Apple Inc.",
            filing_type="10-Q",
            filing_date="2024-08-02",
            filing_url="https://www.sec.gov/example",
        ),
    )

    assert event.event_type == EventType.DOCUMENT_DISCOVERED
    assert event.event_id is not None
    assert event.correlation_id is not None
    assert event.causation_id is None
    assert event.payload.cik == "0000320193"


def test_event_serializes_to_json():
    event = DocumentDiscoveredEvent(
        source_agent=AgentName.WATCHER,
        payload=DocumentDiscoveredPayload(
            cik="0000789019",
            accession_number="0000789019-24-000456",
            company_name="Microsoft Corporation",
            filing_type="10-K",
            filing_date="2024-07-30",
            filing_url="https://www.sec.gov/example",
        ),
    )

    json_data = event.model_dump_json()

    assert "document.discovered" in json_data
    assert "Microsoft Corporation" in json_data
    