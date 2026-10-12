from __future__ import annotations

from functools import cached_property
from typing import Any

from piddiplatsch.core.project_records import (
    BaseProjectRecord,
    ProjectDatasetRecord,
    ProjectFileRecord,
    extract_project_asset_records,
)
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

    @cached_property
    def dataset_properties(self):
        return split_cmip6_id(self.item_id)


def extract_asset_records(
    item: dict[str, Any], exclude_keys: list[str] | None = None
) -> list[CMIP6FileRecord]:
    return list(extract_project_asset_records(item, CMIP6FileRecord, exclude_keys))


__all__ = ["CMIP6DatasetRecord", "CMIP6FileRecord", "extract_asset_records"]
