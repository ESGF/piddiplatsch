from __future__ import annotations

from typing import Any

from piddiplatsch.core.project_records import (
    BaseProjectRecord,
    ProjectDatasetRecord,
    ProjectFileRecord,
    extract_project_asset_records,
)
from piddiplatsch.plugins.cmip7.model import CMIP7DatasetModel, CMIP7FileModel


class BaseCMIP7Record(BaseProjectRecord):
    plugin_name = "cmip7"


class CMIP7FileRecord(ProjectFileRecord, BaseCMIP7Record):
    tracking_id_fields = ("cmip7:tracking_id",)
    dataset_pid_fields = ("cmip7:pid",)
    file_model = CMIP7FileModel


class CMIP7DatasetRecord(ProjectDatasetRecord, BaseCMIP7Record):
    dataset_pid_fields = ("cmip7:pid",)
    dataset_model = CMIP7DatasetModel
    file_record = CMIP7FileRecord


def extract_asset_records(
    item: dict[str, Any], exclude_keys: list[str] | None = None
) -> list[CMIP7FileRecord]:
    return list(extract_project_asset_records(item, CMIP7FileRecord, exclude_keys))


__all__ = ["CMIP7DatasetRecord", "CMIP7FileRecord", "extract_asset_records"]
