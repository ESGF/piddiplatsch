import logging
from datetime import datetime
from functools import cached_property
from pathlib import PurePosixPath
from typing import Any

from piddiplatsch.config import config
from piddiplatsch.core.models import HostingNode
from piddiplatsch.core.records import BaseRecord
from piddiplatsch.exceptions import LookupError
from piddiplatsch.helpers import utc_now
from piddiplatsch.lookup.api import get_lookup
from piddiplatsch.monitoring import stats
from piddiplatsch.plugins.cmip6.model import CMIP6DatasetModel, CMIP6FileModel
from piddiplatsch.utils.models import (
    asset_pid,
    build_handle,
    drop_empty,
    item_pid,
    parse_datetime,
    parse_multihash_checksum,
    parse_pid,
)
from piddiplatsch.utils.stac import Properties, split_cmip6_id

PREFERRED_KEYS = ("reference_file", "data0000", "data0001")


class BaseCMIP6Record(BaseRecord):
    def __init__(
        self,
        item: dict[str, Any],
        additional_attributes: dict[str, Any] | None = None,
    ):
        super().__init__(item)
        self.additional_attributes = additional_attributes or {}

    @cached_property
    def prefix(self) -> str:
        return config.get("handle", {}).get("prefix", "")

    @cached_property
    def landing_page_url(self) -> str:
        return config.get_plugin("cmip6", "landing_page_url", "").rstrip("/")

    @cached_property
    def default_publication_time(self) -> str:
        published_on = self.additional_attributes.get("publication_time")
        if not published_on:
            published_on = utc_now().strftime("%Y-%m-%d %H:%M:%S")
        return published_on

    @cached_property
    def item_id(self) -> str:
        if "id" not in self.item:
            raise KeyError("Missing 'id' in item")
        return self.item["id"]

    @cached_property
    def assets(self) -> dict[str, Any]:
        return self.item.get("assets", {})

    def get_asset(self, key: str) -> dict[str, Any]:
        return self.assets.get(key, {})

    def get_asset_property(self, key: str, prop: str, default: Any = None) -> Any:
        return self.get_asset(key).get(prop, default)

    @cached_property
    def properties(self) -> dict[str, Any]:
        return self.item.get("properties", {})

    def get_property(self, key: str, default: Any = None) -> Any:
        return self.properties.get(key, default)

    @cached_property
    def url(self) -> str:
        return f"{self.landing_page_url}/{self.prefix}/{self.pid}"

    def validate(self):
        try:
            _ = self.model
        except Exception as e:
            raise ValueError(f"Pydantic validation failed: {e}") from e

    def as_record(self) -> dict:
        return drop_empty(self.model.model_dump())

    def as_json(self) -> str:
        return self.model.model_dump_json()


class CMIP6DatasetRecord(BaseCMIP6Record):
    """Wraps a validated CMIP6 STAC item and prepares Handle records."""

    def __init__(
        self,
        item: dict[str, Any],
        exclude_keys: list[str] | None = None,
        additional_attributes: dict[str, Any] | None = None,
    ):
        super().__init__(item, additional_attributes)
        self.exclude_keys = set(exclude_keys or [])
        self.max_parts = config.get_plugin("cmip6", "max_parts", -1)
        self.lookup = get_lookup()

    @cached_property
    def dataset_properties(self) -> Properties:
        return split_cmip6_id(self.item_id)

    @cached_property
    def pid(self) -> str:
        pid_ = parse_pid(self.get_property("pid"))
        if not pid_:
            pid_ = item_pid(self.item_id)
            logging.warning(
                f"Creating new dataset pid: pid={pid_}, ds_id={self.item_id}"
            )
        else:
            logging.info(
                f"Using existing dataset pid: pid={pid_}, ds_id={self.item_id}"
            )
        return pid_

    @cached_property
    def dataset_id(self) -> str:
        parts = self.item_id.rsplit(".", 1)
        return parts[0] if len(parts) > 1 else self.item_id

    @cached_property
    def dataset_version(self) -> str:
        parts = self.item_id.rsplit(".", 1)
        return parts[1] if len(parts) > 1 else ""

    @cached_property
    def has_parts(self) -> list[str]:
        parts = []
        for key in self.assets.keys():
            if key in self.exclude_keys:
                continue
            if self.max_parts > -1 and len(parts) >= self.max_parts:
                logging.warning(f"Reached limit of {self.max_parts} assets.")
                break
            pid = asset_pid(self.item_id, key)
            handle = build_handle(pid, as_uri=True)
            parts.append(handle)
        return parts

    @cached_property
    def is_part_of(self) -> str | None:
        # Placeholder - can be updated to extract actual parent PID if needed
        return None

    @cached_property
    def host(self) -> str:
        for key in PREFERRED_KEYS:
            host = self.get_asset_property(key, "alternate:name")

            if host:
                return host
        return "unknown"

    @cached_property
    def published_on(self) -> str:
        for key in PREFERRED_KEYS:
            published_on = self.get_asset_property(key, "published_on")
            if published_on:
                return parse_datetime(published_on)
        return parse_datetime(self.default_publication_time)

    @cached_property
    def hosting_node(self) -> HostingNode:
        return HostingNode(host=self.host, published_on=self.published_on)

    @cached_property
    def replica_nodes(self) -> list[HostingNode]:
        nodes = []
        known_hosts = set()
        for key in PREFERRED_KEYS:
            alternates = self.get_asset_property(key, "alternate", {})
            for host, values in alternates.items():
                published_on = values.get("published_on") or self.published_on
                if host not in known_hosts:
                    known_hosts.add(host)
                    nodes.append(HostingNode(host=host, published_on=published_on))
        return nodes

    @cached_property
    def retracted(self) -> bool:
        raw = str(self.item.get("properties", {}).get("retracted", "false"))
        return raw.strip().lower() in ("true", "1", "yes")

    @cached_property
    def retracted_on(self) -> datetime | None:
        if self.retracted:
            return parse_datetime(self.default_publication_time)
        return None

    @cached_property
    def previous_version(self) -> str | None:
        """Return the previous version of this dataset.

        Raises:
            LookupError: If the lookup fails.
        """
        try:
            # Backend-agnostic: lookup handles item_id internally
            item_ids = self.lookup.find_versions(self.item_id)
        except LookupError as e:
            logging.error(f"Failed to fetch versions for {self.dataset_id}: {e}")
            raise

        if not item_ids:
            logging.info(f"No versions found for id={self.dataset_id}")
            return None

        current_version = self.dataset_properties.version_number
        for item_id in item_ids:
            version = split_cmip6_id(item_id).version_number
            if version < current_version:
                return item_id

        logging.info(
            f"Dataset id={self.dataset_id} is the latest version {current_version}"
        )
        return None

    def as_handle_model(self) -> CMIP6DatasetModel:
        dsm = CMIP6DatasetModel(
            URL=self.url,
            DATASET_ID=self.dataset_id,
            DATASET_VERSION=self.dataset_version,
            PREVIOUS_VERSION=self.previous_version,
            HAS_PARTS=self.has_parts,
            IS_PART_OF=self.is_part_of,
            HOSTING_NODE=self.hosting_node,
            REPLICA_NODES=self.replica_nodes,
            _RETRACTED=self.retracted,
            RETRACTED_ON=self.retracted_on,
        )

        if self.previous_version:
            logging.info(
                f"Dataset id={self.dataset_id} has previous version {self.previous_version}"
            )

        if self.retracted:
            stats.retracted(f"Dataset id={self.dataset_id} is retracted!")

        if self.replica_nodes:
            stats.replica(
                f"Dataset id={self.dataset_id} has {len(self.replica_nodes)} replica nodes"
            )

        dsm.set_pid(self.pid)
        return dsm


class CMIP6FileRecord(BaseCMIP6Record):
    """Wraps a CMIP6 STAC asset and prepares Handle record for a file."""

    def __init__(self, item: dict[str, Any], asset_key: str):
        super().__init__(item)
        self.asset_key = asset_key

    @cached_property
    def asset(self) -> dict[str, Any]:
        return self.get_asset(self.asset_key)

    @cached_property
    def alternates(self) -> dict[str, Any]:
        return self.asset.get("alternate", {})

    def get_value(self, key: str) -> Any:
        value = self.asset.get(key, "")
        # check alternates if not found
        if not value and self.alternates:
            # pick first available
            for alt in self.alternates.values():
                if alt.get(key):
                    value = alt[key]
                    break
        return value

    @cached_property
    def tracking_id(self) -> str:
        return self.asset.get("tracking_id")

    @cached_property
    def pid(self) -> str:
        pid_ = parse_pid(self.tracking_id)
        if not pid_:
            pid_ = asset_pid(self.item_id, self.asset_key)
            logging.warning(
                f"Creating new file pid: pid={pid_}, asset={self.asset_key}"
            )
        else:
            logging.info(f"Using existing file pid: pid={pid_}, asset={self.asset_key}")
        return pid_

    @cached_property
    def parent(self) -> str:
        return build_handle(item_pid(self.item_id), as_uri=True)

    @cached_property
    def href(self) -> str:
        """Resolved asset URL."""
        return self.get_value("href")

    @cached_property
    def download_url(self) -> str:
        """Alias for href to keep external API consistent."""
        return self.href

    @cached_property
    def replica_download_urls(self) -> list[str]:
        urls = set()
        for alt in self.alternates.values():
            if alt.get("href"):
                urls.add(alt["href"])
        return list(urls)

    @cached_property
    def filename(self) -> str:
        """
        Filename derived from the resolved href.
        """
        # Try filename from resolved href first
        resolved_name = PurePosixPath(self.href).name
        return resolved_name

    @cached_property
    def checksum_with_method(self) -> str | None:
        cval = self.get_value("file:checksum")
        try:
            cmethod, chex = parse_multihash_checksum(cval)
            value = f"{cmethod}:{chex}"
        except Exception:
            value = f"unknown:{cval}"
            logging.warning(f"Could not parse checksum: {cval}")
        return value

    @cached_property
    def checksum(self) -> str | None:
        try:
            chex = self.checksum_with_method.split(":")[1]
        except Exception:
            chex = None
        return chex

    @cached_property
    def checksum_method(self) -> str | None:
        try:
            cmethod = self.checksum_with_method.split(":")[0]
        except Exception:
            cmethod = None
        return cmethod

    @cached_property
    def size(self) -> int | None:
        try:
            return int(self.get_value("file:size"))
        except (ValueError, TypeError):
            return None

    def as_handle_model(self) -> CMIP6FileModel:
        fm = CMIP6FileModel(
            URL=self.url,
            AGGREGATION_LEVEL="FILE",
            IS_PART_OF=self.parent,
            FILE_NAME=self.filename,
            CHECKSUM=self.checksum,
            CHECKSUM_METHOD=self.checksum_method,
            FILE_SIZE=self.size,
            DOWNLOAD_URL=self.download_url,
            REPLICA_DOWNLOAD_URLS=self.replica_download_urls,
        )
        fm.set_pid(self.pid)
        return fm


def extract_asset_records(
    item: dict[str, Any], exclude_keys: list[str]
) -> list[CMIP6FileRecord]:
    """Given a CMIP6 STAC item, return a list of CMIP6FileRecord instances
    for all asset keys except those in exclude_keys.
    """
    exclude_keys = set(exclude_keys or [])
    assets = item.get("assets", {})

    records = []
    for key in assets:
        if key in exclude_keys:
            continue
        try:
            record = CMIP6FileRecord(item, key)
            record.validate()
            records.append(record)
        except ValueError as e:
            logging.warning(f"Skipping asset '{key}': {e}")
    return records


__all__ = [
    "CMIP6DatasetRecord",
    "CMIP6FileRecord",
    "extract_asset_records",
]
