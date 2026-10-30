"""Project-scoped local STAC catalog contracts.

Import storage adapters explicitly, for example ``catalog.sqlite.SQLiteCatalog``,
to keep this package independent of database drivers. Harvest and map select
adapters through ``catalog.api``; the CLI does not select remote lookup backends.
"""

from piddiplatsch.catalog.base import (
    BaseCatalog,
    BaseCatalogReader,
    BaseCatalogTransaction,
)
from piddiplatsch.catalog.models import (
    CatalogEntry,
    KafkaCheckpoint,
    PendingCatalogChanges,
    PendingCatalogItem,
)
from piddiplatsch.exceptions import CatalogError

__all__ = [
    "BaseCatalog",
    "BaseCatalogReader",
    "BaseCatalogTransaction",
    "CatalogEntry",
    "CatalogError",
    "KafkaCheckpoint",
    "PendingCatalogChanges",
    "PendingCatalogItem",
]
