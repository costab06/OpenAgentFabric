from shared.models import AgentName, PipelineEvent, PipelineEventType


def test_pipeline_event_defaults():
    event = PipelineEvent(
        event_type=PipelineEventType.DOCUMENT_DISCOVERED,
        source_agent=AgentName.WATCHER,
    )

    assert event.event_id is not None
    assert event.correlation_id is not None
    assert event.payload == {}