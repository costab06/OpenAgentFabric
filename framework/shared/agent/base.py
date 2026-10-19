
import asyncio
import logging

from abc import ABC, abstractmethod
from collections.abc import Sequence
from uuid import uuid4

from shared.correlation.registry import CorrelationRegistry
from shared.eventbus.base import EventBus
from shared.eventbus.events import EventEnvelope, EventType
from shared.models import AgentName


logger = logging.getLogger(__name__)


class ProcessingOwnershipLost(Exception):
    """Another consumer owns the message, or it is no longer pending."""


class BaseAgent(ABC):
    DEFAULT_STREAM = "oaf.events"
    DEFAULT_RECOVERY_INTERVAL = 10.0
    DEFAULT_MIN_IDLE_MS = 30000
    DEFAULT_RECOVERY_BATCH_SIZE = 100
    DEFAULT_MAX_ATTEMPTS = 5
    DEFAULT_DLQ_STREAM = "oaf.events.dlq"
    DEFAULT_HEARTBEAT_INTERVAL = 10.0

    def __init__(
        self,
        name: AgentName,
        event_bus: EventBus,
        correlation_registry: CorrelationRegistry,
        subscribed_event_types: set[EventType],
        stream: str = DEFAULT_STREAM,
        consumer: str | None = None,
        recovery_interval: float = DEFAULT_RECOVERY_INTERVAL,
        min_idle_ms: int = DEFAULT_MIN_IDLE_MS,
        recovery_batch_size: int = DEFAULT_RECOVERY_BATCH_SIZE,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        dlq_stream: str = DEFAULT_DLQ_STREAM,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
    ):
        if recovery_interval <= 0:
            raise ValueError("recovery_interval must be positive")

        if min_idle_ms < 0:
            raise ValueError("min_idle_ms must be nonnegative")

        if recovery_batch_size < 1:
            raise ValueError("recovery_batch_size must be positive")

        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")

        if heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")

        if (
            min_idle_ms > 0
            and heartbeat_interval * 1000 >= min_idle_ms
        ):
            raise ValueError(
                "heartbeat_interval must be shorter than min_idle_ms"
            )

        self.name = name
        self.event_bus = event_bus
        self.correlation_registry = correlation_registry
        self.subscribed_event_types = subscribed_event_types

        self.stream = stream
        self.group = f"oaf.agent.{name.value}"
        self.consumer = consumer or f"{name.value}-{uuid4().hex}"

        self.recovery_interval = recovery_interval
        self.min_idle_ms = min_idle_ms
        self.recovery_batch_size = recovery_batch_size
        self.max_attempts = max_attempts
        self.dlq_stream = dlq_stream
        self.heartbeat_interval = heartbeat_interval

        self._running = False
        self._processing_lock = asyncio.Lock()

    @abstractmethod
    async def handle_event(
        self,
        event: EventEnvelope,
    ) -> Sequence[EventEnvelope]:
        ...

    def _validate_downstream(
        self,
        source: EventEnvelope,
        downstream_events: Sequence[EventEnvelope],
    ) -> None:
        for downstream in downstream_events:
            if downstream.correlation_id != source.correlation_id:
                raise ValueError(
                    "Downstream correlation_id does not match source"
                )

            if downstream.causation_id != source.event_id:
                raise ValueError(
                    "Downstream causation_id does not match source"
                )

            if downstream.source_agent != self.name:
                raise ValueError(
                    "Downstream source_agent does not match agent"
                )

    async def _heartbeat_loop(
        self,
        message_id: str,
        processing_task: asyncio.Task,
        ownership_lost: asyncio.Event,
        heartbeat_failed: asyncio.Event,
        terminal_phase: asyncio.Event,
    ) -> None:
        """
        Maintain ownership while processing is active.

        A rejected heartbeat means ownership was lost.
        A Redis exception means ownership could not be verified.

        Both conditions stop processing. Neither is classified
        as an application handler failure.
        """
        while True:
            await asyncio.sleep(self.heartbeat_interval)

            try:
                owned = await self.event_bus.heartbeat(
                    stream=self.stream,
                    group=self.group,
                    message_id=message_id,
                    consumer=self.consumer,
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Heartbeat infrastructure failure: %s",
                    message_id,
                )
                heartbeat_failed.set()
                if not terminal_phase.is_set():
                    processing_task.cancel()
                return

            if not owned:
                logger.warning(
                    "Lost ownership of message %s",
                    message_id,
                )
                ownership_lost.set()
                if not terminal_phase.is_set():
                    processing_task.cancel()
                return

    async def _verify_ownership(
        self,
        message_id: str,
    ) -> None:
        owned = await self.event_bus.heartbeat(
            stream=self.stream,
            group=self.group,
            message_id=message_id,
            consumer=self.consumer,
        )

        if not owned:
            raise ProcessingOwnershipLost(
                f"Lost ownership of message {message_id}"
            )

    async def _run_terminal_operation(self, operation):
        """Finish an in-flight terminal operation before honoring cancellation.

        The result may still be uncertain after a network error; the Redis
        terminal-state record must be consulted during subsequent recovery.
        """
        task = asyncio.create_task(operation)
        cancelled = False

        while True:
            try:
                result = await asyncio.shield(task)
                break
            except asyncio.CancelledError:
                # If the inner task itself was cancelled, do not spin.
                if task.done():
                    return await task
                cancelled = True
                logger.warning(
                    "Cancellation during terminal operation; awaiting Redis result"
                )

        if cancelled:
            raise asyncio.CancelledError
        return result

    async def _reconcile_terminal_operation(
        self,
        operation,
        source_event_id,
        expected_state: str,
    ) -> str:
        """Resolve an ambiguous Redis response using the terminal marker.

        An absent marker is *not* proof the operation did not execute.
        Propagate the original exception and let pending recovery retry.
        """
        try:
            await operation
            return expected_state
        except Exception:
            # Do not catch CancelledError: the shielded terminal operation
            # already completed before external cancellation propagates.
            state = await self.event_bus.get_terminal_state(
                self.stream, self.group, source_event_id
            )
            if state in ("committed", "dead_lettered"):
                logger.warning(
                    "Terminal operation response lost for event %s; "
                    "Redis records %s", source_event_id, state
                )
                return state
            raise

    async def _terminal_dead_letter(
        self,
        message_id: str,
        event: EventEnvelope,
        attempts: int,
        error: str,
        terminal_phase: asyncio.Event | None = None,
    ) -> None:
        if terminal_phase is not None:
            terminal_phase.set()
        state = await self._run_terminal_operation(
            self._reconcile_terminal_operation(
                self._dead_letter(message_id, event, attempts, error),
                event.event_id,
                "dead_lettered",
            )
        )
        if state != "dead_lettered":
            logger.warning(
                "Dead-letter attempt for event %s found terminal state %s",
                event.event_id, state,
            )

    async def _dead_letter(
        self,
        message_id: str,
        event: EventEnvelope,
        attempts: int,
        error: str,
    ) -> None:
        moved = await self.event_bus.dead_letter(
            stream=self.stream,
            group=self.group,
            message_id=message_id,
            source_event_id=event.event_id,
            error=error,
            attempts=attempts,
            agent=self.name.value,
            dlq_stream=self.dlq_stream,
            consumer=self.consumer,
        )

        if moved:
            logger.error(
                "Dead-lettered event %s after %d attempts: %s",
                event.event_id,
                attempts,
                error,
            )

    async def process_message(
        self,
        message_id: str,
        event: EventEnvelope,
    ) -> None:
        async with self._processing_lock:
            if event.event_type not in self.subscribed_event_types:
                await self.event_bus.acknowledge(
                    self.stream,
                    self.group,
                    message_id,
                )
                return

            # Check ownership before touching attempt state.
            await self._verify_ownership(message_id)

            existing_attempts = (
                await self.event_bus.get_attempt_count(
                    self.stream,
                    self.group,
                    message_id,
                )
            )

            if existing_attempts >= self.max_attempts:
                await self._terminal_dead_letter(
                    message_id,
                    event,
                    existing_attempts,
                    "Retry budget exhausted after interrupted processing",
                )
                return

            attempts = await self.event_bus.begin_attempt(
                stream=self.stream,
                group=self.group,
                message_id=message_id,
                consumer=self.consumer,
            )

            processing_task = asyncio.current_task()
            ownership_lost = asyncio.Event()
            heartbeat_failed = asyncio.Event()
            terminal_phase = asyncio.Event()

            heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(
                    message_id,
                    processing_task,
                    ownership_lost,
                    heartbeat_failed,
                    terminal_phase,
                ),
                name=f"{self.name.value}-heartbeat",
            )

            try:
                try:
                    correlation = (
                        await self.correlation_registry.get(
                            event.correlation_id
                        )
                    )

                except asyncio.CancelledError:
                    raise

                except Exception:
                    logger.exception(
                        "Correlation registry failure for %s",
                        event.event_id,
                    )
                    raise

                try:
                    if correlation is None:
                        raise ValueError(
                            "Unknown or expired correlation: "
                            f"{event.correlation_id}"
                        )

                    downstream_events = await self.handle_event(
                        event
                    )

                    self._validate_downstream(
                        event,
                        downstream_events,
                    )

                except asyncio.CancelledError:
                    raise

                except Exception as exc:
                    logger.exception(
                        "Handler failure for event %s, attempt %d/%d",
                        event.event_id,
                        attempts,
                        self.max_attempts,
                    )

                    if attempts >= self.max_attempts:
                        await self._verify_ownership(message_id)

                        await self._terminal_dead_letter(
                            message_id,
                            event,
                            attempts,
                            f"{type(exc).__name__}: {exc}",
                            terminal_phase,
                        )

                    return

                # Refresh ownership immediately before commit.
                # This complements the periodic heartbeat.
                await self._verify_ownership(message_id)

                terminal_phase.set()
                state = await self._run_terminal_operation(
                    self._reconcile_terminal_operation(
                        self.event_bus.commit(
                            stream=self.stream,
                            group=self.group,
                            message_id=message_id,
                            source_event_id=event.event_id,
                            downstream_events=downstream_events,
                            consumer=self.consumer,
                        ),
                        event.event_id,
                        "committed",
                    )
                )
                if state != "committed":
                    logger.warning(
                        "Commit for event %s found terminal state %s",
                        event.event_id, state,
                    )
                    return

                try:
                    await self.correlation_registry.refresh(
                        event.correlation_id
                    )

                except asyncio.CancelledError:
                    raise

                except Exception:
                    logger.exception(
                        "Correlation refresh failed after commit "
                        "for event %s",
                        event.event_id,
                    )

            except asyncio.CancelledError:
                if ownership_lost.is_set() and not terminal_phase.is_set():
                    logger.warning(
                        "Processing abandoned after ownership loss: %s",
                        message_id,
                    )
                    return

                if heartbeat_failed.is_set() and not terminal_phase.is_set():
                    logger.error(
                        "Processing abandoned after heartbeat failure: %s",
                        message_id,
                    )
                    return

                raise

            except ProcessingOwnershipLost:
                logger.warning(
                    "Processing abandoned: ownership lost for %s",
                    message_id,
                )

            finally:
                heartbeat_task.cancel()

                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass

    async def _recover_pending_once(self) -> None:
        cursor = "0-0"

        while self._running:
            if self._processing_lock.locked():
                return

            cursor, recovered = (
                await self.event_bus.reclaim_pending_batch(
                    stream=self.stream,
                    group=self.group,
                    consumer=self.consumer,
                    cursor=cursor,
                    min_idle_ms=self.min_idle_ms,
                    count=self.recovery_batch_size,
                )
            )

            for message_id, event in recovered:
                if not self._running:
                    return

                try:
                    await self.process_message(
                        message_id,
                        event,
                    )

                except asyncio.CancelledError:
                    raise

                except Exception:
                    logger.exception(
                        "Recovery failure for message %s",
                        message_id,
                    )

            if cursor == "0-0":
                break

    async def _recovery_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.recovery_interval)

            if not self._running:
                break

            try:
                await self._recover_pending_once()

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Periodic recovery failed for agent %s",
                    self.name.value,
                )

    async def run(self) -> None:
        self._running = True
        recovery_task = None

        try:
            await self._recover_pending_once()

            recovery_task = asyncio.create_task(
                self._recovery_loop(),
                name=f"{self.name.value}-recovery",
            )

            async for message_id, event in (
                self.event_bus.subscribe(
                    stream=self.stream,
                    group=self.group,
                    consumer=self.consumer,
                    block_ms=1000,
                )
            ):
                if not self._running:
                    break

                try:
                    await self.process_message(
                        message_id,
                        event,
                    )

                except asyncio.CancelledError:
                    raise

                except Exception:
                    logger.exception(
                        "Message processing failure for %s",
                        message_id,
                    )

        except asyncio.CancelledError:
            raise

        finally:
            self._running = False

            if recovery_task is not None:
                recovery_task.cancel()

                try:
                    await recovery_task
                except asyncio.CancelledError:
                    pass

    def stop(self) -> None:
        self._running = False
