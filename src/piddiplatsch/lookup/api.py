from piddiplatsch.catalog.base import BaseCatalogReader
from piddiplatsch.config import config
from piddiplatsch.lookup.base import AbstractLookup, DummyLookup
from piddiplatsch.lookup.catalog import CatalogLookup
from piddiplatsch.lookup.stac import STACLookup


def get_lookup(
    project: str | None = None,
    *,
    catalog_reader: BaseCatalogReader | None = None,
    enabled: bool | None = None,
) -> AbstractLookup:
    """
    Factory function that returns the configured lookup instance
    based on [lookup], defaulting to catalog. An injected snapshot always uses
    local lookup; mapping controls enablement through its version-history option.
    """

    # Check for config-based disable flag
    if not (config.get("lookup", "enabled", False) if enabled is None else enabled):
        return DummyLookup()

    backend = (
        "catalog"
        if catalog_reader is not None
        else config.get("lookup", "backend", "catalog").lower()
    )

    if backend == "catalog":
        if project is None:
            raise ValueError("Catalog version lookup requires an explicit project")
        return CatalogLookup(project, reader=catalog_reader)

    if backend == "stac":
        stac_cfg = config.get_stac(project)
        stac_url = stac_cfg.get("base_url")
        collection = stac_cfg.get("collection", "cmip6")
        if not stac_url:
            raise ValueError("STAC backend requires 'stac.base_url' in config")
        return STACLookup(stac_url=stac_url, collection=collection)

    else:
        raise ValueError(f"Unknown lookup backend configured: {backend}")
