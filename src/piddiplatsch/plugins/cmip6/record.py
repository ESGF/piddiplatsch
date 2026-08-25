from __future__ import annotations

import logging
from functools import cached_property
from typing import Any

from piddiplatsch.core.project_records import (
    BaseProjectRecord,
    ProjectDatasetRecord,
    ProjectFileRecord,
    extract_project_asset_records,
)
from piddiplatsch.exceptions import LookupError
from piddiplatsch.lookup.api import get_lookup
from piddiplatsch.plugins.cmip6.model import CMIP6DatasetModel, CMIP6FileModel
from piddiplatsch.utils.stac import split_cmip6_id


class BaseCMIP6Record(BaseProjectRecord):
    plugin_name = "cmip6"


class CMIP6FileRecord(ProjectFileRecord, BaseCMIP6Record):
    tracking_id_fields = ("cmip6:tracking_id", "tracking_id")
    dataset_pid_fields = ("cmip6:pid", "pid")
    file_model = CMIP6FileModel


class CMIP6DatasetRecord(ProjectDatasetRecord, BaseCMIP6Record):
    dataset_pid_fields = ("cmip6:pid", "pid")
    dataset_model = CMIP6DatasetModel
    file_record = CMIP6FileRecord

    def __init__(
        self,
        item: dict[str, Any],
        exclude_keys: list[str] | None = None,
        additional_attributes: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(item, exclude_keys, additional_attributes)
        self.lookup = get_lookup()

    @cached_property
    def dataset_properties(self):
        return split_cmip6_id(self.item_id)

    @cached_property
    def previous_version(self) -> str | None:
        try:
            item_ids = self.lookup.find_versions(self.item_id)
        except LookupError as exc:
            logging.error("Failed to fetch versions for %s: %s", self.dataset_id, exc)
            raise
        if not item_ids:
            return None
        current_version = self.dataset_properties.version_number
        for item_id in item_ids:
            if split_cmip6_id(item_id).version_number < current_version:
                return item_id
        return None


def extract_asset_records(
    item: dict[str, Any], exclude_keys: list[str] | None = None
) -> list[CMIP6FileRecord]:
    return list(extract_project_asset_records(item, CMIP6FileRecord, exclude_keys))


__all__ = ["CMIP6DatasetRecord", "CMIP6FileRecord", "extract_asset_records"]
