"""Contracts for persistent local STAC catalogs; no input or backend adapters."""

from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import AbstractContextManager
from datetime import date
from types import TracebackType
from typing import Self

from piddiplatsch.catalog.models import (
    CatalogEntry,
    KafkaCheckpoint,
    PendingWork,
    WorkItem,
)


class BaseCatalogReader(ABC):
    """Reads scoped to one project, shared by catalogs and write transactions.

    Missing entries/checkpoints return ``None``; backend failures must raise
    ``CatalogError`` rather than look like missing data. Reads return detached
    item dictionaries so patch application cannot mutate stored state implicitly.
    """

    @abstractmethod
    def get_item(self, collection_id: str, item_id: str) -> CatalogEntry | None:
        """Fetch one complete item by exact identity."""

    @abstractmethod
    def find_previous_version(
        self, collection_id: str, dataset_id: str, version_number: int
    ) -> str | None:
        """Return the item ID with the greatest strictly smaller version.

        Search only this project, collection, and dataset using index metadata;
        do not decompress item payloads. Equal versions must not be predecessors.
        """

    @abstractmethod
    def find_versions(
        self, collection_id: str, dataset_id: str, limit: int = 100
    ) -> list[str]:
        """Return indexed item IDs latest-first without reading JSON payloads."""

    @abstractmethod
    def latest_event_date(self) -> date | None:
        """Return the latest dated item's UTC event day."""

    @abstractmethod
    def pending_work(self, max_id: int | None = None) -> PendingWork:
        """Count pending event snapshots without reading payloads."""

    @abstractmethod
    def iter_pending(self, limit: int, max_id: int | None = None) -> Iterator[WorkItem]:
        """Read oldest pending event snapshots from this view, bounded by limit."""

    @abstractmethod
    def get_checkpoint(self, topic: str, partition: int) -> KafkaCheckpoint | None:
        """Read the shared next offset for a source topic/partition."""


class BaseCatalogTransaction(BaseCatalogReader):
    """Writes within a catalog-managed transaction, with read-your-writes.

    Instances are valid only inside ``BaseCatalog.transaction()``. The backend
    serializes or detects conflicting writes; callers own event ordering and
    patch application, not storage adapters.
    """

    @abstractmethod
    def put_item(self, entry: CatalogEntry) -> None:
        """Insert or replace this versioned item, retaining other versions.

        Persist all item fields and replace its index metadata atomically. Do not
        skip an entry just because its collection/item identity already exists.
        """

    @abstractmethod
    def add_work(self, entry: CatalogEntry, source) -> None:
        """Insert an immutable event snapshot in the catalog/checkpoint transaction."""

    @abstractmethod
    def set_checkpoint(self, checkpoint: KafkaCheckpoint) -> None:
        """Write a checkpoint; reject backwards movement with ``CatalogError``."""


class BaseCatalog(BaseCatalogReader):
    """One project's persistent catalog, implemented by a storage backend.

    Project-scoped views share one database file. Project
    routing, STAC validation, version parsing, patches, and Kafka acknowledgement
    remain outside the backend. Importing this interface must not load database
    drivers, configuration, Kafka, or remote clients.
    """

    def __init__(self, project: str) -> None:
        if not isinstance(project, str) or not project:
            raise ValueError("project must be a non-empty string")
        self._project = project

    @property
    def project(self) -> str:
        return self._project

    @abstractmethod
    def transaction(self) -> AbstractContextManager[BaseCatalogTransaction]:
        """Commit all item/checkpoint writes on success; roll back on failure.

        Successful exit means writes are durable. Commit failures raise
        ``CatalogError``. Nested transactions are unsupported and must be rejected.
        """

    @abstractmethod
    def iter_items(
        self,
        *,
        collection_id: str | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
        remote_refresh_only: bool = False,
    ) -> Iterator[CatalogEntry]:
        """Stream entries with bounded memory, optionally by inclusive UTC days.

        Order is collection/item identity, including when filtering by source day. Undated items are excluded from
        date selections. Source-date and recovery filters inspect compressed snapshot metadata.

        ``remote_refresh_only`` selects marked recovery entries.

        Yield one current complete state per versioned item. Callers must close
        the iterator when stopping early; backends release read resources on close.
        Backend documentation must specify concurrent-write/snapshot behavior.
        """

    @abstractmethod
    def reset(self) -> None:
        """Atomically clear this project's items and checkpoints.

        Reject reset during an active transaction. This does not unlink files or
        promise physical compaction; file removal belongs to catalog management.
        """

    @abstractmethod
    def seal_batch(self, batch_id: str, outcomes: list[dict]) -> None:
        """Commit exact mapping outcomes after batch files are durable, idempotently."""

    @abstractmethod
    def publish_batch(self, batch_id: str, handle_count: int, *, error=None) -> None:
        """Update publication progress and the retained batch summary atomically."""

    @abstractmethod
    def compact(self) -> None:
        """Reclaim unused storage without changing entries or checkpoints.

        Reject compaction during active transactions or local read iterators.
        Backends must report external reader/writer conflicts as ``CatalogError``.
        """

    @abstractmethod
    def close(self) -> None:
        """Release resources; repeated calls are safe, later operations raise.

        Active uncommitted writes must never be committed by closing the catalog.
        """

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
