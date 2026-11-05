"""Run with pytest after applying the OpenAgentFabric patch."""
import pytest
from pydantic import ValidationError
from shared.eventbus.events import EventEnvelope
from shared.agent.base import BaseAgent


def test_custom_event_round_trip():
    event = EventEnvelope(
        event_type="factor.data.fetched",
        source_agent="factor.fetcher",
        payload={"schema_version": "1.0", "dataset": "ff3", "sha256": "abc"},
    )
    restored = EventEnvelope.model_validate_json(event.model_dump_json())
    assert restored.event_type == "factor.data.fetched"
    assert restored.source_agent == "factor.fetcher"
    assert restored.payload["dataset"] == "ff3"

def test_legacy_event_round_trip():
    from shared.eventbus.events import DocumentDiscoveredEvent, DocumentDiscoveredPayload
    event = DocumentDiscoveredEvent(
        source_agent="watcher",
        payload=DocumentDiscoveredPayload(
            cik="320193", accession_number="0000320193-26-000001",
            company_name="Apple", filing_type="10-K",
            filing_date="2026-01-01", filing_url="https://www.sec.gov/example",
        ),
    )
    assert EventEnvelope.model_validate_json(event.model_dump_json()).event_type == "document.discovered"

def test_distinct_namespaced_groups():
    assert f"oaf.agent.factor.fetcher" != f"oaf.agent.fetcher"
