"""Optional source coordinates and acknowledgement for processing inputs."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class KafkaSource:
    topic: str
    partition: int
    offset: int

    def __post_init__(self) -> None:
        if not isinstance(self.topic, str) or not self.topic:
            raise ValueError("Kafka source requires a topic")
        for name in ("partition", "offset"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"Kafka {name} must be a non-negative integer")


@dataclass(frozen=True)
class SourceMessage:
    """Payload plus source position and a signal that processing is durable.

    ``acknowledge`` may queue a broker offset for a later batch commit. It must
    only be called after successful processing; input adapters own final flushing.
    """

    key: str | None
    value: dict[str, Any]
    source: KafkaSource
    acknowledge: Callable[[], None] | None = None
