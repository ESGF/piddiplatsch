from __future__ import annotations

import time
from datetime import date
from typing import Any, ClassVar

from pydantic import ValidationError

from piddiplatsch.catalog.base import BaseCatalogReader
from piddiplatsch.config import config
from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.core.project_records import ProjectDatasetRecord, ProjectFileRecord
from piddiplatsch.core.registry import get_plugin
from piddiplatsch.lookup.catalog import CatalogLookup
from piddiplatsch.result import ProcessingResult


class StacProjectProcessor(BaseProcessor):
    """Shared publication-envelope and Handle-writing workflow for STAC projects."""

    plugin_name: ClassVar[str]
    dataset_record: ClassVar[type[ProjectDatasetRecord]]
    file_record: ClassVar[type[ProjectFileRecord]]
    default_excluded_asset_keys: ClassVar[list[str]] = [
        "reference_file",
        "globus",
        "thumbnail",
        "quicklook",
    ]

    def __init__(
        self,
        excluded_asset_keys=None,
        *,
        catalog_reader: BaseCatalogReader | None = None,
        version_history: bool = False,
        require_all_assets: bool = False,
        **kwargs,
    ):
        kwargs["project"] = self.plugin_name
        super().__init__(**kwargs)
        self.excluded_asset_keys = excluded_asset_keys or config.get_plugin(
            self.plugin_name,
            "excluded_asset_keys",
            self.default_excluded_asset_keys,
        )
        self.catalog_reader = catalog_reader
        self.version_history = version_history
        self.require_all_assets = require_all_assets
        self.version_lookup = (
            CatalogLookup(self.plugin_name, reader=catalog_reader)
            if catalog_reader is not None
            else None
        )

    def process(self, key: str, value: dict[str, Any]) -> ProcessingResult:
        self.logger.debug("%s plugin processing key=%s", self.plugin_name, key)
        started = time.perf_counter()
        result = ProcessingResult(key=key)
        try:
            result = self._do_process(value, key, result)
        except ValidationError as exc:
            self.logger.error("Validation error for key=%s: %s", key, exc)
            raise
        except Exception as exc:
            self.logger.error("Processing error for key=%s: %s", key, exc)
            raise
        result.elapsed = time.perf_counter() - started
        result.success = not result.skipped
        return result

    def _do_process(
        self, value: dict[str, Any], key: str, result: ProcessingResult
    ) -> ProcessingResult:
        if not isinstance(value, dict):
            raise ValueError("Publication message must be an object")
        data = value.get("data")
        if not isinstance(data, dict):
            raise ValueError("MISSING data object")
        payload = data.get("payload")
        if not isinstance(payload, dict) or not payload:
            raise ValueError("MISSING payload object")
        metadata = value.get("metadata") or {}
        if not isinstance(metadata, dict):
            raise ValueError("Publication metadata must be an object")
        return self._process_payload(payload, metadata, key, result)

    def _process_payload(
        self,
        payload: dict[str, Any],
        metadata: dict[str, Any],
        key: str,
        result: ProcessingResult,
    ) -> ProcessingResult:
        if payload.get("method") == "PATCH":
            raise ValueError(
                "Mapping requires complete STAC items; apply patches during harvest"
            )
        elif isinstance(payload.get("item"), dict):
            item = payload["item"]
        else:
            raise ValueError("MISSING item object")

        previous_version = metadata.get("previous_version")
        if self.catalog_reader is not None:
            setter = getattr(self.handle_backend, "set_output_date", None)
            if callable(setter):
                day = metadata.get("catalog_date")
                setter(date.fromisoformat(day) if day else None)
            previous_version = None
            if self.version_history:
                dataset_id, version_number = get_plugin(self.plugin_name).catalog_index(
                    item["id"]
                )
                collection_id = payload.get("collection_id") or item.get("collection")
                previous_version = self.version_lookup.find_previous_version(
                    collection_id, dataset_id, version_number
                )
            # Catalog inputs are synthetic envelopes owned by the mapping runner.
            # Preserve resolved history in diagnostic failure records.
            metadata["previous_version"] = previous_version
        record = self.dataset_record(
            item,
            exclude_keys=self.excluded_asset_keys,
            additional_attributes={
                "publication_time": metadata.get("time"),
                "previous_version": previous_version,
            },
            handle_profile=self.handle_profile,
        )
        record.validate()
        result.num_handles, result.handle_processing_time = self._add_records_from_item(
            record, item
        )
        result.num_alias_handles = int(record.alias_record is not None)
        return result

    def _add_records_from_item(
        self, record: ProjectDatasetRecord, item: dict[str, Any]
    ) -> tuple[int, float]:
        def add_records():
            alias = record.alias_record
            if alias is not None:
                alias.validate()
            self._safe_add_record(record)
            count = 1
            if alias is not None:
                self._safe_add_record(alias)
                self.logger.debug(
                    "Prepared dataset alias Handle: pid=%s target=%s",
                    alias.pid,
                    alias.url,
                )
                count += 1
            excluded = set(self.excluded_asset_keys)
            assets = item.get("assets") or {}
            if not isinstance(assets, dict):
                raise ValueError("STAC item 'assets' must be an object")
            for asset_key in assets:
                if asset_key in excluded:
                    continue
                asset_record = self.file_record(
                    item,
                    asset_key,
                    handle_profile=self.handle_profile,
                )
                try:
                    asset_record.validate()
                except ValueError as exc:
                    if self.require_all_assets:
                        raise ValueError(f"Invalid asset {asset_key!r}: {exc}") from exc
                    self.logger.warning("Skipping asset '%s': %s", asset_key, exc)
                    continue
                self._safe_add_record(asset_record)
                count += 1
            return count

        return self._time_function(add_records)


__all__ = ["StacProjectProcessor"]
