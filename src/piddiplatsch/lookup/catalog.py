"""Dataset version lookup using the project's indexed local catalog."""

from contextlib import contextmanager
from time import perf_counter

from piddiplatsch.catalog.api import open_catalog
from piddiplatsch.catalog.base import BaseCatalogReader
from piddiplatsch.config import config
from piddiplatsch.core.registry import get_plugin
from piddiplatsch.lookup.base import AbstractLookup


class CatalogLookup(AbstractLookup):
    """Reuse an injected snapshot, or open/close a read-only catalog per query.

    The caller owns an injected reader. This adapter never fetches remote STAC,
    scans JSON, creates a database, or retains a connection it opened itself.
    """

    def __init__(
        self,
        project: str,
        *,
        reader: BaseCatalogReader | None = None,
        collection: str | None = None,
    ):
        self.plugin = get_plugin(project)
        self.reader = reader
        self.collection = collection or config.get_stac(self.plugin.name).get(
            "collection"
        )
        self.previous_queries = 0
        self.previous_found = 0
        self.previous_missing = 0
        self.previous_seconds = 0.0

    @contextmanager
    def _snapshot(self):
        if self.reader is not None:
            yield self.reader
        else:
            with open_catalog(self.plugin.name, read_only=True) as reader:
                yield reader

    def find_versions(
        self, item_id: str, limit: int = 100, *, collection_id: str | None = None
    ) -> list[str]:
        collection = collection_id or self.collection
        if not collection:
            raise ValueError("Catalog version lookup requires a collection")
        dataset_id, _ = self.plugin.catalog_index(item_id)
        with self._snapshot() as reader:
            return reader.find_versions(collection, dataset_id, limit)

    def find_previous_version(
        self, collection_id: str, dataset_id: str, version_number: int
    ) -> str | None:
        """Query the exact nearest predecessor using the existing version index."""
        started = perf_counter()
        self.previous_queries += 1
        try:
            with self._snapshot() as reader:
                previous = reader.find_previous_version(
                    collection_id, dataset_id, version_number
                )
        finally:
            self.previous_seconds += perf_counter() - started
        if previous is None:
            self.previous_missing += 1
        else:
            self.previous_found += 1
        return previous
