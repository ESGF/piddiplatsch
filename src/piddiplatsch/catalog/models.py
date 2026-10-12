"""Storage-neutral metadata for a project-scoped STAC catalog."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CatalogEntry:
    """A complete STAC item with plugin-derived version index metadata.

    ``dataset_id`` excludes the version; ``version_number`` is a sortable integer
    supplied by the project plugin, not inferred by the storage backend. The item
    retains all source fields. Its dictionary remains mutable; backends must
    persist a snapshot and return detached dictionaries on reads.
    """

    collection_id: str
    item: dict[str, Any]
    dataset_id: str
    version_number: int

    def __post_init__(self) -> None:
        if not isinstance(self.collection_id, str) or not self.collection_id:
            raise ValueError("collection_id must be a non-empty string")
        if not isinstance(self.item, dict):
            raise ValueError("item must be an object")
        if not isinstance(self.item.get("id"), str) or not self.item["id"]:
            raise ValueError("item requires a non-empty string 'id'")
        if "collection" in self.item and self.item["collection"] != self.collection_id:
            raise ValueError("item collection must match collection_id")
        if not isinstance(self.dataset_id, str) or not self.dataset_id:
            raise ValueError("dataset_id must be a non-empty string")
        if type(self.version_number) is not int or self.version_number < 0:
            raise ValueError("version_number must be a non-negative integer")

    @property
    def item_id(self) -> str:
        return self.item["id"]


@dataclass(frozen=True)
class KafkaCheckpoint:
    """Next offset to consume in one topic/partition for this project.

    This is a local durable checkpoint, independent of Kafka group commits.
    Advancing it requires durable handling of every preceding consumed record,
    including any unresolved records. It must never move backwards during normal
    harvesting; replay from an earlier offset requires an explicit reset.
    """

    topic: str
    partition: int
    next_offset: int

    def __post_init__(self) -> None:
        if not isinstance(self.topic, str) or not self.topic:
            raise ValueError("topic must be a non-empty string")
        for name in ("partition", "next_offset"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
