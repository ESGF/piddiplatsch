from __future__ import annotations

import uuid
from datetime import datetime
from typing import ClassVar

from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    PositiveInt,
    PrivateAttr,
    field_serializer,
    model_validator,
)

from piddiplatsch.config import config
from piddiplatsch.core.models import ALLOWED_CHECKSUM_METHODs, HostingNode, strict_mode
from piddiplatsch.monitoring import stats


class BaseHandleModel(BaseModel):
    """Fields and validation shared by all project Handle records."""

    _PID: str | None = PrivateAttr(default=None)
    ESGF: str = "ESGF2 TEST"
    URL: HttpUrl

    @field_serializer("URL")
    def _serialize_url(self, value: HttpUrl) -> str:
        return str(value)

    def set_pid(self, value: str | uuid.UUID) -> None:
        if isinstance(value, uuid.UUID):
            self._PID = str(value)
            return
        if isinstance(value, str):
            try:
                self._PID = str(uuid.UUID(value))
                return
            except ValueError as exc:
                raise ValueError(
                    f"Invalid PID string: {value} is not a valid UUID."
                ) from exc
        raise TypeError(
            f"PID must be a UUID or UUID string, got {type(value).__name__}"
        )

    def get_pid(self) -> str | None:
        return self._PID


class DatasetHandleModel(BaseHandleModel):
    """Project-neutral ESGF dataset Handle schema."""

    plugin_name: ClassVar[str | None] = None
    AGGREGATION_LEVEL: str = "DATASET"
    DATASET_ID: str
    DATASET_VERSION: str | None = None
    PREVIOUS_VERSION: str | None = None
    IS_PART_OF: str | None = None
    HAS_PARTS: list[str] = Field(default_factory=list)
    HOSTING_NODE: HostingNode
    REPLICA_NODES: list[HostingNode] = Field(default_factory=list)
    _RETRACTED: bool | None = PrivateAttr(default=False)
    RETRACTED_ON: datetime | None = None

    @model_validator(mode="after")
    def validate_required(self) -> DatasetHandleModel:
        max_parts = -1
        if self.plugin_name:
            max_parts = config.get_plugin(self.plugin_name, "max_parts", -1)
        if max_parts != 0 and not self.HAS_PARTS and self.RETRACTED_ON is None:
            if strict_mode():
                raise ValueError("HAS_PARTS must contain at least one file.")
            stats.warn(message="HAS_PARTS must contain at least one file.")
        if max_parts > 0 and len(self.HAS_PARTS) > max_parts:
            raise ValueError(
                f"Too many parts: {len(self.HAS_PARTS)} exceeds max_parts={max_parts}"
            )
        return self


class FileHandleModel(BaseHandleModel):
    """Project-neutral ESGF file Handle schema."""

    AGGREGATION_LEVEL: str = "FILE"
    FILE_NAME: str
    IS_PART_OF: str
    CHECKSUM: str
    CHECKSUM_METHOD: str
    FILE_SIZE: PositiveInt
    DOWNLOAD_URL: HttpUrl
    REPLICA_DOWNLOAD_URLS: list[HttpUrl] = Field(default_factory=list)

    @field_serializer("DOWNLOAD_URL", "REPLICA_DOWNLOAD_URLS")
    def _serialize_urls(self, value) -> str | list[str]:
        if isinstance(value, list):
            return [str(url) for url in value]
        return str(value)

    @model_validator(mode="after")
    def validate_checksum(self) -> FileHandleModel:
        if not self.CHECKSUM:
            raise ValueError("CHECKSUM is required.")
        if not self.CHECKSUM_METHOD:
            raise ValueError("CHECKSUM_METHOD is required.")
        if self.CHECKSUM_METHOD not in ALLOWED_CHECKSUM_METHODs:
            message = f"Used CHECKSUM_METHOD is not allowed: {self.CHECKSUM_METHOD}"
            if strict_mode():
                raise ValueError(message)
            stats.warn(message=message)
        return self


__all__ = ["BaseHandleModel", "DatasetHandleModel", "FileHandleModel"]
