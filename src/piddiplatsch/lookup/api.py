from piddiplatsch.config import config
from piddiplatsch.lookup.base import AbstractLookup, DummyLookup
from piddiplatsch.lookup.stac import STACLookup


def get_lookup(project: str | None = None) -> AbstractLookup:
    """
    Factory function that returns the configured lookup instance
    based on the TOML config [lookup] section.
    """

    # Check for config-based disable flag
    if not config.get("lookup", "enabled", False):
        return DummyLookup()

    backend = config.get("lookup", "backend", "stac").lower()

    if backend == "stac":
        stac_cfg = config.get_stac(project)
        stac_url = stac_cfg.get("base_url")
        collection = stac_cfg.get("collection", "cmip6")
        if not stac_url:
            raise ValueError("STAC backend requires 'stac.base_url' in config")
        return STACLookup(stac_url=stac_url, collection=collection)

    else:
        raise ValueError(f"Unknown lookup backend configured: {backend}")
