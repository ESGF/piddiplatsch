from __future__ import annotations

import logging
from datetime import datetime
from functools import cached_property
from pathlib import PurePosixPath
from typing import Any, ClassVar

from piddiplatsch.config import config
from piddiplatsch.core.handle_models import DatasetHandleModel, FileHandleModel
from piddiplatsch.core.models import HostingNode
from piddiplatsch.core.records import BaseRecord
from piddiplatsch.helpers import utc_now
from piddiplatsch.monitoring import stats
from piddiplatsch.utils.models import (
    asset_pid,
    build_handle,
    item_pid,
    parse_datetime,
    parse_multihash_checksum,
    parse_pid,
)

PREFERRED_ASSET_KEYS = ("reference_file", "data0000", "data0001")


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"STAC item '{name}' must be an object")
    return value


def _source_pid(container: dict[str, Any], fields: tuple[str, ...]) -> str | None:
    """Resolve one PID from canonical and compatibility fields.

    More than one populated field is accepted only when all values identify the
    same Handle suffix. This prevents silently choosing a conflicting PID.
    """
    found = [(field, parse_pid(container.get(field))) for field in fields]
    found = [(field, value) for field, value in found if value]
    if not found:
        return None
    values = {value for _, value in found}
    if len(values) > 1:
        details = ", ".join(f"{field}={container[field]!r}" for field, _ in found)
        raise ValueError(f"Conflicting source PID fields: {details}")
    return found[0][1]


class BaseProjectRecord(BaseRecord):
    plugin_name: ClassVar[str]

    def __init__(
        self,
        item: dict[str, Any],
        additional_attributes: dict[str, Any] | None = None,
    ) -> None:
        if not isinstance(item, dict):
            raise ValueError("STAC item must be an object")
        super().__init__(item)
        self.additional_attributes = additional_attributes or {}

    @cached_property
    def prefix(self) -> str:
        return config.get("handle", {}).get("prefix", "")

    @cached_property
    def landing_page_url(self) -> str:
        return config.get_plugin(self.plugin_name, "landing_page_url", "").rstrip("/")

    @cached_property
    def default_publication_time(self) -> str:
        value = self.additional_attributes.get("publication_time")
        return value or utc_now().strftime("%Y-%m-%d %H:%M:%S")

    @cached_property
    def item_id(self) -> str:
        value = self.item.get("id")
        if not isinstance(value, str) or not value:
            raise ValueError("STAC item requires a non-empty string 'id'")
        return value

    @cached_property
    def assets(self) -> dict[str, Any]:
        return _mapping(self.item.get("assets"), "assets")

    def get_asset(self, key: str) -> dict[str, Any]:
        return _mapping(self.assets.get(key), f"assets.{key}")

    def get_asset_property(self, key: str, prop: str, default: Any = None) -> Any:
        return self.get_asset(key).get(prop, default)

    @cached_property
    def properties(self) -> dict[str, Any]:
        return _mapping(self.item.get("properties"), "properties")

    @cached_property
    def url(self) -> str:
        return f"{self.landing_page_url}/{self.prefix}/{self.pid}"


class ProjectDatasetRecord(BaseProjectRecord):
    dataset_pid_fields: ClassVar[tuple[str, ...]]
    dataset_model: ClassVar[type[DatasetHandleModel]] = DatasetHandleModel
    file_record: ClassVar[type[ProjectFileRecord]]

    def __init__(
        self,
        item: dict[str, Any],
        exclude_keys: list[str] | None = None,
        additional_attributes: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(item, additional_attributes)
        self.exclude_keys = set(exclude_keys or [])
        self.max_parts = config.get_plugin(self.plugin_name, "max_parts", -1)

    @cached_property
    def pid(self) -> str:
        value = _source_pid(self.properties, self.dataset_pid_fields)
        if value:
            logging.info(
                "Using existing dataset pid: pid=%s, ds_id=%s", value, self.item_id
            )
            return value
        value = item_pid(self.item_id)
        logging.warning(
            "Creating new dataset pid: pid=%s, ds_id=%s", value, self.item_id
        )
        return value

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
        parts: list[str] = []
        for key in self.assets:
            if key in self.exclude_keys:
                continue
            if self.max_parts > -1 and len(parts) >= self.max_parts:
                logging.warning("Reached limit of %s assets.", self.max_parts)
                break
            file_pid = self.file_record(self.item, key).pid
            parts.append(build_handle(file_pid, as_uri=True))
        return parts

    @cached_property
    def is_part_of(self) -> str | None:
        return None

    @cached_property
    def ordered_asset_keys(self) -> list[str]:
        preferred = [key for key in PREFERRED_ASSET_KEYS if key in self.assets]
        return preferred + [key for key in self.assets if key not in preferred]

    @cached_property
    def host(self) -> str:
        for key in self.ordered_asset_keys:
            host = self.get_asset_property(key, "alternate:name")
            if host:
                return host
        return "unknown"

    @cached_property
    def published_on(self) -> datetime | None:
        for key in self.ordered_asset_keys:
            asset = self.get_asset(key)
            value = (
                asset.get("published_on")
                or asset.get("created")
                or asset.get("updated")
            )
            if value:
                return parse_datetime(value)
        value = self.properties.get("created") or self.properties.get("updated")
        return parse_datetime(value or self.default_publication_time)

    @cached_property
    def hosting_node(self) -> HostingNode:
        return HostingNode(host=self.host, published_on=self.published_on)

    @cached_property
    def replica_nodes(self) -> list[HostingNode]:
        nodes: list[HostingNode] = []
        known_hosts: set[str] = set()
        for key in self.ordered_asset_keys:
            alternates = self.get_asset_property(key, "alternate", {})
            if not isinstance(alternates, dict):
                continue
            for host, values in alternates.items():
                values = values if isinstance(values, dict) else {}
                published_on = (
                    parse_datetime(values.get("published_on")) or self.published_on
                )
                if host not in known_hosts:
                    known_hosts.add(host)
                    nodes.append(HostingNode(host=host, published_on=published_on))
        return nodes

    @cached_property
    def retracted(self) -> bool:
        raw = str(self.properties.get("retracted", "false"))
        return raw.strip().lower() in ("true", "1", "yes")

    @cached_property
    def retracted_on(self) -> datetime | None:
        return parse_datetime(self.default_publication_time) if self.retracted else None

    @cached_property
    def previous_version(self) -> str | None:
        return None

    def as_handle_model(self) -> DatasetHandleModel:
        model = self.dataset_model(
            URL=self.url,
            DATASET_ID=self.dataset_id,
            DATASET_VERSION=self.dataset_version,
            PREVIOUS_VERSION=self.previous_version,
            HAS_PARTS=self.has_parts,
            IS_PART_OF=self.is_part_of,
            HOSTING_NODE=self.hosting_node,
            REPLICA_NODES=self.replica_nodes,
            RETRACTED_ON=self.retracted_on,
        )
        if self.retracted:
            stats.retracted(f"Dataset id={self.dataset_id} is retracted!")
        if self.replica_nodes:
            stats.replica(
                f"Dataset id={self.dataset_id} has {len(self.replica_nodes)} replica nodes"
            )
        model.set_pid(self.pid)
        return model


class ProjectFileRecord(BaseProjectRecord):
    tracking_id_fields: ClassVar[tuple[str, ...]]
    dataset_pid_fields: ClassVar[tuple[str, ...]]
    file_model: ClassVar[type[FileHandleModel]] = FileHandleModel

    def __init__(self, item: dict[str, Any], asset_key: str) -> None:
        super().__init__(item)
        self.asset_key = asset_key

    @cached_property
    def asset(self) -> dict[str, Any]:
        return self.get_asset(self.asset_key)

    @cached_property
    def alternates(self) -> dict[str, Any]:
        return _mapping(
            self.asset.get("alternate"), f"assets.{self.asset_key}.alternate"
        )

    def get_value(self, key: str) -> Any:
        value = self.asset.get(key, "")
        if not value:
            for alternate in self.alternates.values():
                if isinstance(alternate, dict) and alternate.get(key):
                    return alternate[key]
        return value

    @cached_property
    def tracking_id(self) -> str | None:
        return next(
            (
                self.asset.get(key)
                for key in self.tracking_id_fields
                if self.asset.get(key)
            ),
            None,
        )

    @cached_property
    def pid(self) -> str:
        value = _source_pid(self.asset, self.tracking_id_fields)
        if value:
            logging.info(
                "Using existing file pid: pid=%s, asset=%s", value, self.asset_key
            )
            return value
        value = asset_pid(self.item_id, self.asset_key)
        logging.warning(
            "Creating new file pid: pid=%s, asset=%s", value, self.asset_key
        )
        return value

    @cached_property
    def parent(self) -> str:
        value = _source_pid(self.properties, self.dataset_pid_fields) or item_pid(
            self.item_id
        )
        return build_handle(value, as_uri=True)

    @cached_property
    def href(self) -> str:
        return self.get_value("href")

    @cached_property
    def download_url(self) -> str:
        return self.href

    @cached_property
    def replica_download_urls(self) -> list[str]:
        return sorted(
            {
                alternate["href"]
                for alternate in self.alternates.values()
                if isinstance(alternate, dict) and alternate.get("href")
            }
        )

    @cached_property
    def filename(self) -> str:
        return PurePosixPath(self.href).name

    @cached_property
    def checksum_with_method(self) -> str:
        value = self.get_value("file:checksum")
        try:
            method, checksum = parse_multihash_checksum(value)
            return f"{method}:{checksum}"
        except Exception:
            logging.warning("Could not parse checksum: %s", value)
            return f"unknown:{value}"

    @cached_property
    def checksum(self) -> str:
        return self.checksum_with_method.partition(":")[2]

    @cached_property
    def checksum_method(self) -> str:
        return self.checksum_with_method.partition(":")[0]

    @cached_property
    def size(self) -> int | None:
        try:
            return int(self.get_value("file:size"))
        except (ValueError, TypeError):
            return None

    def as_handle_model(self) -> FileHandleModel:
        model = self.file_model(
            URL=self.url,
            IS_PART_OF=self.parent,
            FILE_NAME=self.filename,
            CHECKSUM=self.checksum,
            CHECKSUM_METHOD=self.checksum_method,
            FILE_SIZE=self.size,
            DOWNLOAD_URL=self.download_url,
            REPLICA_DOWNLOAD_URLS=self.replica_download_urls,
        )
        model.set_pid(self.pid)
        return model


def extract_project_asset_records(
    item: dict[str, Any],
    record_type: type[ProjectFileRecord],
    exclude_keys: list[str] | None = None,
) -> list[ProjectFileRecord]:
    excluded = set(exclude_keys or [])
    assets = _mapping(item.get("assets"), "assets")
    records: list[ProjectFileRecord] = []
    for key in assets:
        if key in excluded:
            continue
        try:
            record = record_type(item, key)
            record.validate()
            records.append(record)
        except ValueError as exc:
            logging.warning("Skipping asset '%s': %s", key, exc)
    return records


__all__ = [
    "BaseProjectRecord",
    "ProjectDatasetRecord",
    "ProjectFileRecord",
    "extract_project_asset_records",
]
