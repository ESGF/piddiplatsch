import time
from typing import Any

import jsonpatch
import requests
from pydantic import ValidationError

from piddiplatsch.config import config
from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.exceptions import TransientExternalError
from piddiplatsch.plugins.cmip6.record import CMIP6DatasetRecord, extract_asset_records
from piddiplatsch.result import ProcessingResult
from piddiplatsch.utils.stac import get_stac_client


class CMIP6Processor(BaseProcessor):
    """CMIP6-specific processor logic."""

    EXCLUDED_ASSET_KEYS = ["reference_file", "globus", "thumbnail", "quicklook"]

    def __init__(self, excluded_asset_keys=None, **kwargs):
        super().__init__(**kwargs)
        self.excluded_asset_keys = excluded_asset_keys or config.get_plugin(
            "cmip6", "excluded_asset_keys", self.EXCLUDED_ASSET_KEYS
        )
        self.stac_client = get_stac_client()

    def preflight_check(self, stop_on_transient_skip: bool = True):
        """Optionally check STAC availability before consuming.

        If remote STAC is configured and unreachable, raise TransientExternalError
        when policy dictates fail-fast.
        """
        stac_cfg = config.get("stac", {})
        base_url = stac_cfg.get("base_url")
        consumer_cfg = config.get("consumer", {})
        transient_cfg = consumer_cfg.get("transient", {})
        preflight = bool(
            transient_cfg.get(
                "preflight_stac",
                consumer_cfg.get("preflight_stac", True),
            )
        )
        timeout = float(stac_cfg.get("timeout", 10.0))
        if not preflight or not base_url:
            return
        try:
            # lightweight probe
            url = base_url.rstrip("/") + "/collections?limit=1"
            resp = requests.get(url, timeout=min(timeout, 3.0))
            resp.raise_for_status()
        except Exception as e:
            if stop_on_transient_skip:
                raise TransientExternalError(f"STAC preflight failed: {e}")
            # else, just log and continue in force/dev mode
            self.logger.warning(f"STAC preflight failed but continuing: {e}")

    def process(self, key: str, value: dict[str, Any]) -> ProcessingResult:
        self.logger.debug(f"CMIP6 plugin processing key={key}")
        start_total = time.perf_counter()

        result = ProcessingResult(key=key)

        try:
            result = self._do_process(value, key, result)
        except ValidationError as e:
            self.logger.error(f"Validation error for key={key}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Processing error for key={key}: {e}")
            raise

        result.elapsed = time.perf_counter() - start_total
        result.success = not result.skipped

        return result

    def _do_process(
        self, value: dict[str, Any], key: str, result: ProcessingResult
    ) -> ProcessingResult:
        """Check payload presence and delegate to payload processor."""
        payload = value.get("data", {}).get("payload")
        if not payload:
            raise ValueError("MISSING payload")

        metadata = value.get("metadata", {})
        return self._process_payload(payload, metadata, key, result)

    def _process_payload(
        self,
        payload: dict[str, Any],
        metadata: dict[str, Any],
        key: str,
        result: ProcessingResult,
    ) -> ProcessingResult:
        """Decide how to process the payload: PATCH or full item."""
        if payload.get("method") == "PATCH":
            try:
                item = self._apply_patch_to_stac_item(payload)
                self.logger.info(f"Patched item with key={key}.")
                result.patched = True
            except TransientExternalError as e:
                self.logger.error(f"External failure during patch for key={key}: {e}")
                result.skipped = True
                result.skip_reason = f"TRANSIENT external: {e}"
                result.transient_skip = True
                return result
            except jsonpatch.JsonPatchException as e:
                self.logger.error(f"Invalid JSON Patch for key={key}: {e}")
                raise
            except Exception as e:
                self.logger.error(f"Failed to apply patch for key={key}: {e}")
                raise
        elif "item" in payload:
            item = payload["item"]
        else:
            raise ValueError("MISSING item")

        # Create record
        additional_attrs = {"publication_time": metadata.get("time")}
        record = CMIP6DatasetRecord(
            item,
            exclude_keys=self.excluded_asset_keys,
            additional_attributes=additional_attrs,
        )
        # Validate record
        try:
            record.validate()
        except Exception as e:
            self.logger.error(f"Validation failed for key={key}: {e}")
            raise

        # Handle processing
        num_handles, handle_time = self._add_records_from_item(record, item)
        result.num_handles = num_handles
        result.handle_processing_time = handle_time

        return result

    def _apply_patch_to_stac_item(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Fetch the STAC item and apply a JSON patch with retries and backoff.

        Raises TransientExternalError if STAC is unreachable or returns 5xx after retries.
        Raises JsonPatchException for invalid patch specs.
        """
        collection_id = payload["collection_id"]
        item_id = payload["item_id"]
        patch_data = payload["patch"]
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
        base_delay = float(
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

        last_err: Exception | None = None
        delay = base_delay
        for attempt in range(1, retries + 2):
            try:
                item = self.stac_client.get_item(collection_id, item_id)
                if item is None:
                    raise requests.HTTPError(
                        f"404 Not Found for {collection_id}/{item_id}"
                    )
                patch_obj = jsonpatch.JsonPatch(patch_data["operations"])
                patched_item = patch_obj.apply(item)
                self.logger.info(
                    f"Applied patch to STAC item {collection_id}/{item_id}"
                )
                self.logger.debug(f"patched item: {patched_item}")
                return patched_item
            except requests.Timeout as e:
                last_err = e
                self.logger.warning(
                    f"Timeout fetching STAC item (attempt {attempt}): {e}"
                )
            except requests.ConnectionError as e:
                last_err = e
                self.logger.warning(
                    f"Connection error fetching STAC item (attempt {attempt}): {e}"
                )
            except requests.HTTPError as e:
                last_err = e
                self.logger.warning(
                    f"HTTP error fetching STAC item (attempt {attempt}): {e}"
                )
            except jsonpatch.JsonPatchException:
                # invalid patch is permanent error
                raise
            except Exception as e:
                last_err = e
                self.logger.warning(
                    f"Unexpected error fetching STAC item (attempt {attempt}): {e}"
                )

            # backoff before next attempt (only for transient/classified errors)
            if attempt <= retries:
                time.sleep(delay)
                delay = min(delay * 2, max_delay)

        raise TransientExternalError(
            f"Failed to fetch/apply patch for {collection_id}/{item_id} after {retries + 1} attempts: {last_err}"
        )

    def _add_records_from_item(
        self, record: CMIP6DatasetRecord, item: dict[str, Any]
    ) -> tuple[int, float]:
        """Add main record and extracted asset records, measuring handle processing time."""

        def add_records():
            self._safe_add_record(record)
            num_handles = 1
            for r in extract_asset_records(item, exclude_keys=self.excluded_asset_keys):
                self._safe_add_record(r)
                num_handles += 1
            return num_handles

        num_handles, handle_time = self._time_function(add_records)
        return num_handles, handle_time

    def _log_skipped(self, key: str, reason: str):
        """Standardized logging for skipped messages."""
        self.logger.warning(f"[{reason} skipped] key={key}")
