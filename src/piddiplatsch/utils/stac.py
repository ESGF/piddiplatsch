from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import requests
from pystac import Item

from piddiplatsch.config import config  # your TOML config loader


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


class BaseStacClient(ABC):
    @abstractmethod
    def get_item(self, collection_id: str, item_id: str) -> dict[str, Any]:
        """Fetch a full STAC item."""
        pass


class RemoteStacClient(BaseStacClient):
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get_item(self, collection_id: str, item_id: str) -> dict[str, Any]:
        url = f"{self.base_url}/collections/{collection_id}/items/{item_id}"
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()


class LocalStacClient(BaseStacClient):
    """Dummy/local STAC client for testing."""

    def get_item(self, collection_id: str, item_id: str) -> dict[str, Any]:
        return {
            "id": item_id,
            "type": "Feature",
            "collection": collection_id,
            "properties": {},
            "links": [],
            "assets": {},
        }


def get_stac_client(project: str | None = None) -> BaseStacClient:
    """
    Factory function to return a STAC client based on the config.

    Config example:
    [stac]
    base_url = "https://api.stac.esgf.ceda.ac.uk"
    """
    stac_cfg = config.get_stac(project)
    base_url = stac_cfg.get("base_url")
    timeout = float(stac_cfg.get("timeout", 10.0))
    if not base_url:
        return LocalStacClient()
    return RemoteStacClient(base_url, timeout=timeout)
