from __future__ import annotations

import time
from typing import Any, ClassVar

import jsonpatch
import requests
from pydantic import ValidationError

from piddiplatsch.config import config
from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.core.project_records import ProjectDatasetRecord, ProjectFileRecord
from piddiplatsch.exceptions import TransientExternalError
from piddiplatsch.result import ProcessingResult
from piddiplatsch.utils.stac import get_stac_client


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

    def __init__(self, excluded_asset_keys=None, **kwargs):
        kwargs["project"] = self.plugin_name
        super().__init__(**kwargs)
        self.excluded_asset_keys = excluded_asset_keys or config.get_plugin(
            self.plugin_name,
            "excluded_asset_keys",
            self.default_excluded_asset_keys,
        )
        self.stac_client = get_stac_client()

    def preflight_check(self, stop_on_transient_skip: bool = True):
        stac_cfg = config.get("stac", {})
        base_url = stac_cfg.get("base_url")
        consumer_cfg = config.get("consumer", {})
        transient_cfg = consumer_cfg.get("transient", {})
        preflight = bool(
            transient_cfg.get(
                "preflight_stac", consumer_cfg.get("preflight_stac", True)
            )
        )
        timeout = float(stac_cfg.get("timeout", 10.0))
        if not preflight or not base_url:
            return
        try:
            response = requests.get(
                base_url.rstrip("/") + "/collections?limit=1",
                timeout=min(timeout, 3.0),
            )
            response.raise_for_status()
        except Exception as exc:
            if stop_on_transient_skip:
                raise TransientExternalError(f"STAC preflight failed: {exc}") from exc
            self.logger.warning("STAC preflight failed but continuing: %s", exc)

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
            try:
                item = self._apply_patch_to_stac_item(payload)
                result.patched = True
            except TransientExternalError as exc:
                result.skipped = True
                result.skip_reason = f"TRANSIENT external: {exc}"
                result.transient_skip = True
                return result
        elif isinstance(payload.get("item"), dict):
            item = payload["item"]
        else:
            raise ValueError("MISSING item object")

        record = self.dataset_record(
            item,
            exclude_keys=self.excluded_asset_keys,
            additional_attributes={"publication_time": metadata.get("time")},
        )
        record.validate()
        result.num_handles, result.handle_processing_time = self._add_records_from_item(
            record, item
        )
        return result

    def _apply_patch_to_stac_item(self, payload: dict[str, Any]) -> dict[str, Any]:
        collection_id = payload["collection_id"]
        item_id = payload["item_id"]
        patch_data = payload["patch"]
        # The production publisher sends the RFC 6902 operation list directly.
        # Keep the wrapped form readable for older captured/test messages.
        operations = (
            patch_data.get("operations") if isinstance(patch_data, dict) else patch_data
        )
        if not isinstance(operations, list):
            raise ValueError("PATCH payload requires a patch operation list")

        consumer_cfg = config.get("consumer", {})
        transient_cfg = consumer_cfg.get("transient", {})
        retries = int(
            transient_cfg.get(
                "retries",
                transient_cfg.get(
                    "transient_retries", consumer_cfg.get("transient_retries", 3)
                ),
            )
        )
        delay = float(
            transient_cfg.get(
                "backoff_initial",
                transient_cfg.get(
                    "transient_backoff_initial",
                    consumer_cfg.get("transient_backoff_initial", 0.5),
                ),
            )
        )
        max_delay = float(
            transient_cfg.get(
                "backoff_max",
                transient_cfg.get(
                    "transient_backoff_max",
                    consumer_cfg.get("transient_backoff_max", 5.0),
                ),
            )
        )

        last_error: Exception | None = None
        for attempt in range(1, retries + 2):
            try:
                item = self.stac_client.get_item(collection_id, item_id)
                if item is None:
                    raise requests.HTTPError(
                        f"404 Not Found for {collection_id}/{item_id}"
                    )
                return jsonpatch.JsonPatch(operations).apply(item)
            except jsonpatch.JsonPatchException:
                raise
            except Exception as exc:
                last_error = exc
                self.logger.warning(
                    "Failed to fetch/patch STAC item (attempt %s): %s", attempt, exc
                )
            if attempt <= retries:
                time.sleep(delay)
                delay = min(delay * 2, max_delay)

        raise TransientExternalError(
            f"Failed to fetch/apply patch for {collection_id}/{item_id} "
            f"after {retries + 1} attempts: {last_error}"
        )

    def _add_records_from_item(
        self, record: ProjectDatasetRecord, item: dict[str, Any]
    ) -> tuple[int, float]:
        def add_records():
            self._safe_add_record(record)
            count = 1
            excluded = set(self.excluded_asset_keys)
            assets = item.get("assets") or {}
            if not isinstance(assets, dict):
                raise ValueError("STAC item 'assets' must be an object")
            for asset_key in assets:
                if asset_key in excluded:
                    continue
                asset_record = self.file_record(item, asset_key)
                try:
                    asset_record.validate()
                except ValueError as exc:
                    self.logger.warning("Skipping asset '%s': %s", asset_key, exc)
                    continue
                self._safe_add_record(asset_record)
                count += 1
            return count

        return self._time_function(add_records)


__all__ = ["StacProjectProcessor"]
