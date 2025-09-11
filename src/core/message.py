from dataclasses import dataclass, field
from typing import Any
import datetime


@dataclass(frozen=True)
class Message:
    """
    Represents a standardized, immutable message within the bridge.
    Encapsulating all data and metadata of an event.
    """

    source_id: str
    topic: str
    payload: bytes

    timestamp: datetime.datetime = field(default_factory=datetime.datetime.utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)
