"""Project-scoped local STAC catalog contracts.

Import storage adapters explicitly, for example ``catalog.sqlite.SQLiteCatalog``,
to keep this package independent of database drivers. Existing harvest, mapping,
and remote lookup paths are not wired to the catalog yet.
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
