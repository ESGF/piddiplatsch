"""Remote current-item recovery for harvest; mapping never uses this reader."""

from datetime import UTC, datetime

from piddiplatsch.config import config
from piddiplatsch.exceptions import CatalogError
from piddiplatsch.utils.stac import RemoteStacClient


def fetch_current_item(
    project: str, collection_id: str, item_id: str
) -> tuple[dict, dict]:
    """Fetch a real remote item and record provenance, without inventing a base."""
    settings = config.get_stac(project)
    endpoint = settings.get("base_url")
    if not endpoint:
        raise CatalogError(
            f"Missing PATCH base item: {collection_id}/{item_id}; remote refresh requires a configured STAC base_url"
        )
    try:
        item = RemoteStacClient(
            endpoint, timeout=float(settings.get("timeout", 10.0))
        ).get_item(collection_id, item_id)
    except Exception as exc:
        raise CatalogError(
            f"Remote refresh failed for {collection_id}/{item_id}: {exc}"
        ) from exc
    if not isinstance(item, dict) or item.get("id") != item_id:
        raise CatalogError(
            f"Remote refresh returned the wrong item: {collection_id}/{item_id}"
        )
    if item.get("collection") != collection_id:
        raise CatalogError(
            f"Remote refresh returned the wrong collection: {collection_id}/{item_id}"
        )
    return item, {
        "mode": "current-item",
        "endpoint": endpoint,
        "fetched_at": datetime.now(UTC).isoformat(),
    }
