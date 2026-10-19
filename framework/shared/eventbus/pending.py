
from dataclasses import dataclass


@dataclass(frozen=True)
class PendingMessage:
    message_id: str
    consumer: str
    delivery_count: int
    idle_ms: int
