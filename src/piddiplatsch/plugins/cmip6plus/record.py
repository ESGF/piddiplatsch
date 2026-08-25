from __future__ import annotations

from typing import Any

from piddiplatsch.core.project_records import (
    BaseProjectRecord,
    ProjectDatasetRecord,
    ProjectFileRecord,
    extract_project_asset_records,
)
from piddiplatsch.plugins.cmip6plus.model import (
    CMIP6PlusDatasetModel,
    CMIP6PlusFileModel,
)


class BaseCMIP6PlusRecord(BaseProjectRecord):
    plugin_name = "cmip6plus"


class CMIP6PlusFileRecord(ProjectFileRecord, BaseCMIP6PlusRecord):
    tracking_id_fields = ("cmip6plus:tracking_id",)
    dataset_pid_fields = ("cmip6plus:pid",)
    file_model = CMIP6PlusFileModel


class CMIP6PlusDatasetRecord(ProjectDatasetRecord, BaseCMIP6PlusRecord):
    dataset_pid_fields = ("cmip6plus:pid",)
    dataset_model = CMIP6PlusDatasetModel
    file_record = CMIP6PlusFileRecord


def extract_asset_records(
    item: dict[str, Any], exclude_keys: list[str] | None = None
) -> list[CMIP6PlusFileRecord]:
    return list(extract_project_asset_records(item, CMIP6PlusFileRecord, exclude_keys))


__all__ = ["CMIP6PlusDatasetRecord", "CMIP6PlusFileRecord", "extract_asset_records"]
