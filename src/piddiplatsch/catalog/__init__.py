"""Project-scoped local STAC catalog contracts.

Import storage adapters explicitly, for example ``catalog.sqlite.SQLiteCatalog``,
to keep this package independent of database drivers. Harvest uses the SQLite
adapter explicitly; mapping and remote lookup paths remain independent for now.
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
