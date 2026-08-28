from piddiplatsch.config import config
from piddiplatsch.lookup.base import AbstractLookup, DummyLookup
from piddiplatsch.lookup.es import ElasticsearchLookup
from piddiplatsch.lookup.stac import STACLookup


def get_lookup() -> AbstractLookup:
    """
    Factory function that returns the configured lookup instance
    based on the TOML config [lookup] section.
    """

    # Check for config-based disable flag
    if not config.get("lookup", "enabled", False):
        return DummyLookup()

    backend = config.get("lookup", "backend", "stac").lower()

    if backend == "stac":
        stac_url = config.get("stac", "base_url")
        collection = config.get("stac", "collection", "cmip6")
        if not stac_url:
            raise ValueError("STAC backend requires 'stac.base_url' in config")
        return STACLookup(stac_url=stac_url, collection=collection)

    elif backend in ("elasticsearch", "es"):
        es_url = config.get("elasticsearch", "base_url")
        index = config.get("elasticsearch", "index", "handle_21t14995")
        if not es_url:
            raise ValueError(
                "Elasticsearch backend requires 'elasticsearch.base_url' in config"
            )
        return ElasticsearchLookup(es_url=es_url, index=index)

    else:
        raise ValueError(f"Unknown lookup backend configured: {backend}")
