"""Storage-neutral metadata for a project-scoped STAC catalog."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any


@dataclass(frozen=True)
class CatalogEntry:
    """A complete STAC item with plugin-derived version index metadata.

    ``dataset_id`` excludes the version; ``version_number`` is a sortable integer
    supplied by the project plugin, not inferred by the storage backend. The item
    ``remote_refresh`` is local recovery provenance, separate from source JSON.
    A non-None value marks the item for current-state remote refresh in harvest.
    The item retains all source fields. Its dictionary remains mutable; backends must
    persist a snapshot and return detached dictionaries on reads.
    """

    collection_id: str
    item: dict[str, Any]
    dataset_id: str
    version_number: int
    publication_time: str | None = None
    remote_refresh: dict[str, Any] | None = None

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
        if self.publication_time is not None and not isinstance(
            self.publication_time, str
        ):
            raise ValueError("publication_time must be a string or None")

        if self.remote_refresh is not None and not isinstance(
            self.remote_refresh, dict
        ):
            raise ValueError("remote_refresh must be an object or None")

    @property
    def item_id(self) -> str:
        return self.item["id"]

    @property
    def event_date(self) -> date | None:
        """UTC day of the latest event, falling back to STAC update/create time.

        Naive timestamps are treated as UTC. Undated/invalid values never acquire
        today's date just because mapping is repeated.
        """
        properties = self.item.get("properties")
        properties = properties if isinstance(properties, dict) else {}
        for value in (
            self.publication_time,
            properties.get("updated"),
            properties.get("created"),
        ):
            if not isinstance(value, str) or not value:
                continue
            try:
                timestamp = datetime.fromisoformat(value)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=UTC)
                return timestamp.astimezone(UTC).date()
            except (ValueError, OverflowError):
                continue
        return None


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


@dataclass(frozen=True)
class PendingCatalogChanges:
    """Distinct dirty items and their oldest pending time in a catalog snapshot."""

    generation: str
    count: int
    oldest_at: datetime | None
    sequence: int


@dataclass(frozen=True)
class PendingCatalogItem:
    entry: CatalogEntry
    sequence: int
