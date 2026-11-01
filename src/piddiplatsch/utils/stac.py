from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

if TYPE_CHECKING:
    from pystac import Item


@dataclass
class Properties:
    activity_id: str
    institution_id: str
    source_id: str
    experiment_id: str
    variant_label: str
    table_id: str
    variable_id: str
    grid_label: str
    version: str
    version_number: int


def split_cmip6_id(item_id: str) -> Properties:
    """Split CMIP6 dataset-id into structured properties."""
    parts = item_id.split(".")
    if len(parts) < 10:
        raise ValueError(
            f"Invalid CMIP6 dataset-id format: {item_id}. "
            f"Expected format: <project>.<activity_id>.<institution_id>.<source_id>."
            f"<experiment_id>.<variant_label>.<table_id>.<variable_id>.<grid_label>.<version>"
        )
    return Properties(
        activity_id=parts[1],
        institution_id=parts[2],
        source_id=parts[3],
        experiment_id=parts[4],
        variant_label=parts[5],
        table_id=parts[6],
        variable_id=parts[7],
        grid_label=parts[8],
        version=parts[-1],
        version_number=int(parts[-1][1:]),
    )


def extract_version(item: Item) -> int:
    """Extract the integer version number from a CMIP6 dataset-id (e.g., vYYYYMMDD)."""
    return int(item.id.split(".")[-1][1:])


class RemoteStacClient:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get_item(self, collection_id: str, item_id: str) -> dict[str, Any]:
        import requests

        url = f"{self.base_url}/collections/{quote(collection_id, safe='')}/items/{quote(item_id, safe='')}"
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()
