"""Reconstruct project catalogs from publication events without Handle mapping."""

from contextlib import ExitStack
from pathlib import Path

import jsonpatch

from piddiplatsch.catalog.base import BaseCatalog, BaseCatalogTransaction
from piddiplatsch.catalog.models import CatalogEntry, KafkaCheckpoint
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.core.plugin import normalize_project_id
from piddiplatsch.core.registry import get_plugins
from piddiplatsch.core.routing import extract_project_id
from piddiplatsch.exceptions import CatalogError
from piddiplatsch.result import ProcessingResult


class CatalogHarvester:
    """Harvest all registered projects, opening their catalogs on first use.

    Failures stop consumption so later offsets cannot hide unresolved events.
    Each item must be ordered within one source partition; cross-partition/global
    ordering and rebuild management are outside this initial integration.
    """

    requires_kafka_source = True
    stop_on_error = True

    def __init__(self, directory: Path, *, backend: str = "sqlite"):
        if backend != "sqlite":
            raise ValueError(f"Unsupported catalog backend: {backend!r}")
        self.directory = Path(directory)
        self.plugins = get_plugins("all")
        self._plugins_by_project = {
            project_id: plugin
            for plugin in self.plugins
            for project_id in plugin.project_ids
        }
        self._catalogs: dict[str, BaseCatalog] = {}

    @property
    def project_names(self) -> tuple[str, ...]:
        return tuple(plugin.name for plugin in self.plugins)

    def __str__(self) -> str:
        return "harvest"

    def preflight_check(self, stop_on_transient_skip: bool = True) -> None:
        return None

    def plugin_name_for(self, value: dict) -> str | None:
        project = extract_project_id(value)
        plugin = self._plugins_by_project.get(normalize_project_id(project or ""))
        return plugin.name if plugin else None

    def _get_catalog(self, project: str) -> BaseCatalog:
        if project not in self._catalogs:
            # Plugin names become filenames; reject unsafe third-party names.
            if Path(project).name != project or project in {".", ".."}:
                raise CatalogError(f"Unsafe catalog project name: {project!r}")
            from piddiplatsch.catalog.sqlite import SQLiteCatalog

            self._catalogs[project] = SQLiteCatalog(
                project, self.directory / f"{project}.sqlite"
            )
        return self._catalogs[project]

    def process(self, key: str, value: dict) -> ProcessingResult:
        raise CatalogError("Catalog harvesting requires Kafka source coordinates")

    @staticmethod
    def _payload(value: dict) -> dict:
        if not isinstance(value, dict) or not isinstance(value.get("data"), dict):
            raise ValueError("Publication requires a data object")
        if value["data"].get("type", "STAC") != "STAC":
            raise ValueError("Publication data type must be STAC")
        if not isinstance(value["data"].get("payload"), dict):
            raise ValueError("Publication requires a payload object")
        return value["data"]["payload"]

    @staticmethod
    def _apply_patch(payload: dict, transaction: BaseCatalogTransaction) -> dict:
        collection_id = payload.get("collection_id")
        item_id = payload.get("item_id")
        if not isinstance(collection_id, str) or not collection_id:
            raise ValueError("PATCH requires collection_id")
        if not isinstance(item_id, str) or not item_id:
            raise ValueError("PATCH requires item_id")
        patch = payload.get("patch")
        operations = patch.get("operations") if isinstance(patch, dict) else patch
        if not isinstance(operations, list):
            raise ValueError("PATCH requires a patch operation list")
        base = transaction.get_item(collection_id, item_id)
        if base is None:
            raise CatalogError(f"Missing PATCH base item: {collection_id}/{item_id}")
        try:
            item = jsonpatch.JsonPatch(operations).apply(base.item)
        except (jsonpatch.JsonPatchException, jsonpatch.JsonPointerException) as exc:
            raise CatalogError(
                f"Could not apply PATCH for {collection_id}/{item_id}: {exc}"
            ) from exc
        if not isinstance(item, dict) or item.get("id") != item_id:
            raise ValueError("PATCH must preserve item identity")
        return item

    def process_with_source(
        self, key: str, value: dict, source: KafkaSource
    ) -> ProcessingResult:
        payload = self._payload(value)
        project = self.plugin_name_for(value)
        if project is None:
            raise CatalogError("Publication project is missing or not registered")
        plugin = next(plugin for plugin in self.plugins if plugin.name == project)
        catalog = self._get_catalog(project)
        result = ProcessingResult(
            key=key, success=True, project=project, plugin=project
        )
        with catalog.transaction() as transaction:
            checkpoint = transaction.get_checkpoint(source.topic, source.partition)
            if checkpoint is not None and source.offset < checkpoint.next_offset:
                result.filtered = True
                result.filtered_reason = "Kafka message already harvested"
                return result
            collection_id = payload.get("collection_id")
            method = payload.get("method", "POST")
            if method == "PATCH":
                item = self._apply_patch(payload, transaction)
                result.patched = True
            elif method == "POST":
                item = payload.get("item")
                if isinstance(item, dict):
                    collection_id = collection_id or item.get("collection")
                    if "item_id" in payload and payload["item_id"] != item.get("id"):
                        raise ValueError("Publication item_id must match item identity")
            else:
                raise ValueError(f"Unsupported publication method: {method!r}")
            if not isinstance(item, dict) or item.get("type") != "Feature":
                raise ValueError("Publication requires a STAC Feature item")
            for field in ("properties", "assets"):
                if not isinstance(item.get(field), dict):
                    raise ValueError(f"STAC item {field} must be an object")
            if "links" in item and not isinstance(item["links"], list):
                raise ValueError("STAC item links must be a list")
            # Cross-check routing again after patch application.
            checked = {
                "data": {"payload": {"collection_id": collection_id, "item": item}}
            }
            if self.plugin_name_for(checked) != project:
                raise ValueError("Patched item must preserve project identity")
            item_id = item.get("id")
            if not isinstance(item_id, str) or not item_id:
                raise ValueError("STAC item requires a non-empty string 'id'")
            dataset_id, version_number = plugin.catalog_index(item_id)
            transaction.put_item(
                CatalogEntry(collection_id, item, dataset_id, version_number)
            )
            transaction.set_checkpoint(
                KafkaCheckpoint(source.topic, source.partition, source.offset + 1)
            )
        return result

    def close(self) -> None:
        try:
            with ExitStack() as stack:
                for catalog in self._catalogs.values():
                    stack.callback(catalog.close)
        finally:
            self._catalogs.clear()
