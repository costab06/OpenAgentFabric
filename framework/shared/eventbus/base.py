
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from uuid import UUID
from shared.eventbus.pending import PendingMessage


from shared.eventbus.events import EventEnvelope


class EventBus(ABC):
    """
    Abstract interface for OpenAgentFabric event transports.
    """

    @abstractmethod
    async def publish(
        self,
        stream: str,
        event: EventEnvelope,
    ) -> str:
        """Publish an event and return its transport message ID."""
        ...

    @abstractmethod
    async def subscribe(
        self,
        stream: str,
        group: str,
        consumer: str,
        count: int = 10,
        block_ms: int = 5000,
    ) -> AsyncIterator[tuple[str, EventEnvelope]]:
        """Continuously yield messages assigned to a consumer."""
        ...

    @abstractmethod
    async def acknowledge(
        self,
        stream: str,
        group: str,
        message_id: str,
    ) -> None:
        """Acknowledge a successfully processed message."""
        ...

    @abstractmethod
    async def reclaim_pending(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_ms: int = 30_000,
        count: int = 100,
    ) -> list[tuple[str, EventEnvelope]]:
        """Reclaim abandoned messages from the consumer group."""
        ...

    @abstractmethod
    async def commit(
            self,
            stream: str,
            group: str,
            message_id: str,
            source_event_id: UUID,
            downstream_events: Sequence[EventEnvelope],
            consumer: str | None = None,
    ) -> bool:
        ...
        """
        Atomically publish downstream events and acknowledge the input.

        Return True for a new commit, False for an already committed event.
        """
        ...


    @abstractmethod
    async def dead_letter(
            self,
            stream: str,
            group: str,
            message_id: str,
            source_event_id: UUID,
            error: str,
            attempts: int,
            agent: str,
            dlq_stream: str = "oaf.events.dlq",
            consumer: str | None = None,
    ) -> bool:
        ...
        """
        Atomically move a failed pending message to the DLQ.

        Returns True when moved, False if already moved.
        """
        ...

    @abstractmethod
    async def begin_attempt(
        self,
        stream: str,
        group: str,
        message_id: str,
        consumer: str,
    ) -> int:
        """
        Atomically record the start of a processing attempt.

        Return the new attempt count.

        The caller must own the pending message.
        """
        ...

    @abstractmethod
    async def get_attempt_count(
        self,
        stream: str,
        group: str,
        message_id: str,
    ) -> int:
        """Return the persisted processing-attempt count."""
        ...

    @abstractmethod
    async def clear_attempts(
        self,
        stream: str,
        group: str,
        message_id: str,
    ) -> None:
        """Remove processing-attempt state."""
        ...

    @abstractmethod
    async def get_pending_message(
        self,
        stream: str,
        group: str,
        message_id: str,
    ) -> PendingMessage | None:
        """
        Return pending-message ownership and delivery information.

        Return None if the message is not pending in the group.
        """
        ...

    @abstractmethod
    async def get_terminal_state(
        self,
        stream: str,
        group: str,
        source_event_id: str,
    ) -> str | None:
        """
        Return 'committed', 'dead_lettered', or None.

        None means there is no recorded terminal state.
        It does not necessarily mean the operation failed.
        """
        ...

    @abstractmethod
    async def health(self) -> bool:
        """Check transport availability."""
        ...
