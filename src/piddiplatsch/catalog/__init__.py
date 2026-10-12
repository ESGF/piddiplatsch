"""Project-scoped local STAC catalog contracts.

SQLite and DuckDB adapters will implement these contracts. Existing harvest,
mapping, and remote lookup paths are not wired to the catalog yet.
"""

from piddiplatsch.catalog.base import (
    BaseCatalog,
    BaseCatalogReader,
    BaseCatalogTransaction,
)
from piddiplatsch.catalog.models import CatalogEntry, KafkaCheckpoint
from piddiplatsch.exceptions import CatalogError

__all__ = [
    "BaseCatalog",
    "BaseCatalogReader",
    "BaseCatalogTransaction",
    "CatalogEntry",
    "CatalogError",
    "KafkaCheckpoint",
]
