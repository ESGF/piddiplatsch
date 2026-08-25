from __future__ import annotations

from typing import Any

from piddiplatsch.core.project_records import (
    BaseProjectRecord,
    ProjectDatasetRecord,
    ProjectFileRecord,
    extract_project_asset_records,
)
from piddiplatsch.plugins.cordex_cmip6.model import (
    CordexCMIP6DatasetModel,
    CordexCMIP6FileModel,
)


class BaseCordexCMIP6Record(BaseProjectRecord):
    plugin_name = "cordex-cmip6"


class CordexCMIP6FileRecord(ProjectFileRecord, BaseCordexCMIP6Record):
    tracking_id_fields = ("cordex-cmip6:tracking_id",)
    dataset_pid_fields = ("cordex-cmip6:pid",)
    file_model = CordexCMIP6FileModel


class CordexCMIP6DatasetRecord(ProjectDatasetRecord, BaseCordexCMIP6Record):
    dataset_pid_fields = ("cordex-cmip6:pid",)
    dataset_model = CordexCMIP6DatasetModel
    file_record = CordexCMIP6FileRecord


def extract_asset_records(
    item: dict[str, Any], exclude_keys: list[str] | None = None
) -> list[CordexCMIP6FileRecord]:
    return list(
        extract_project_asset_records(item, CordexCMIP6FileRecord, exclude_keys)
    )


__all__ = [
    "CordexCMIP6DatasetRecord",
    "CordexCMIP6FileRecord",
    "extract_asset_records",
]
