"""Reconstruct project catalogs from publication events without Handle mapping."""

import logging
from contextlib import ExitStack, closing
from pathlib import Path

import jsonpatch

from piddiplatsch.catalog.api import get_catalog_path, open_catalog
from piddiplatsch.catalog.base import BaseCatalog, BaseCatalogTransaction
from piddiplatsch.catalog.models import CatalogEntry, KafkaCheckpoint
from piddiplatsch.catalog.remote import fetch_current_item
from piddiplatsch.catalog.state import HarvestState
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.core.plugin import normalize_project_id
from piddiplatsch.core.registry import get_plugins
from piddiplatsch.core.routing import extract_project_id
from piddiplatsch.exceptions import CatalogError
from piddiplatsch.result import ProcessingResult

LOGGER = logging.getLogger(__name__)


class CatalogHarvester:
    """Harvest all registered projects, opening their catalogs on first use.

    Failures stop consumption so later offsets cannot hide unresolved events.
    Each item must be ordered within one source partition; cross-partition/global
    ordering is not established by recovery. Missing patch bases recover current
    remote state; marked entries remain in remote-refresh mode until rebuilt.
    """

    requires_kafka_source = True
    stop_on_error = True

    def __init__(
        self,
        directory: Path,
        *,
        backend: str = "sqlite",
        clean_projects: tuple[str, ...] = (),
    ):
        if backend != "sqlite":
            raise ValueError(f"Unsupported catalog backend: {backend!r}")
        self.directory = Path(directory)
        self.backend = backend
        self.plugins = get_plugins("all")
        self._plugins_by_project = {
            project_id: plugin
            for plugin in self.plugins
            for project_id in plugin.project_ids
        }
        self._catalogs: dict[str, BaseCatalog] = {}
        self._state = HarvestState(self.directory)
        try:
            known_projects = set(self.project_names)
            if set(clean_projects) - known_projects:
                raise ValueError("Clean projects must be registered plugin names")
            if clean_projects:
                self._state.request_clean(clean_projects)
            # Upgrade existing catalogs even when Kafka has no new messages.
            for project in self.project_names:
                if get_catalog_path(
                    project, directory=self.directory, backend=self.backend
                ).is_file():
                    self._get_catalog(project)
            if set(self._state.pending_clean) - known_projects:
                raise CatalogError("Pending clean references an unregistered project")
            for project in self._state.pending_clean:
                # Do not create empty databases for projects never harvested.
                if get_catalog_path(
                    project, directory=self.directory, backend=self.backend
                ).exists():
                    catalog = self._get_catalog(project)
                    catalog.reset()
                    catalog.compact()
            self._state.complete_clean()
            self._refresh_recovered_items()
        except BaseException:
            self.close()
            raise

    @property
    def kafka_group_suffix(self) -> str:
        return self._state.kafka_group_suffix

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
            self._catalogs[project] = open_catalog(
                project, directory=self.directory, backend=self.backend
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

    def _apply_patch(
        self,
        project: str,
        payload: dict,
        transaction: BaseCatalogTransaction,
        source: KafkaSource,
    ) -> tuple[dict, dict | None]:
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
        # Validate operations even when recovery deliberately does not apply them.
        try:
            jsonpatch.JsonPatch(operations)
        except (jsonpatch.JsonPatchException, TypeError) as exc:
            raise ValueError(f"Invalid PATCH operations: {exc}") from exc
        if base is None or base.remote_refresh is not None:
            return self._remote_item(project, collection_id, item_id, source, base)
        try:
            item = jsonpatch.JsonPatch(operations).apply(base.item)
        except (jsonpatch.JsonPatchException, jsonpatch.JsonPointerException) as exc:
            raise CatalogError(
                f"Could not apply PATCH for {collection_id}/{item_id}: {exc}"
            ) from exc
        if not isinstance(item, dict) or item.get("id") != item_id:
            raise ValueError("PATCH must preserve item identity")
        return item, None

    def process_with_source(
        self, key: str, value: dict, source: KafkaSource
    ) -> ProcessingResult:
        payload = self._payload(value)
        metadata = value.get("metadata") or {}
        if not isinstance(metadata, dict):
            raise ValueError("Publication metadata must be an object")
        project = self.plugin_name_for(value)
        if project is None:
            raise CatalogError("Publication project is missing or not registered")
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
            recovery = None
            method = payload.get("method", "POST")
            if method == "PATCH":
                item, recovery = self._apply_patch(
                    project, payload, transaction, source
                )
                result.patched = True
            elif method == "POST":
                item = payload.get("item")
                if isinstance(item, dict):
                    collection_id = collection_id or item.get("collection")
                    if "item_id" in payload and payload["item_id"] != item.get("id"):
                        raise ValueError("Publication item_id must match item identity")
            else:
                raise ValueError(f"Unsupported publication method: {method!r}")
            entry = self._validated_entry(
                project, collection_id, item, metadata.get("time"), recovery
            )
            if method == "POST":
                base = transaction.get_item(entry.collection_id, entry.item_id)
                if base is not None and base.remote_refresh is not None:
                    item, recovery = self._remote_item(
                        project, entry.collection_id, entry.item_id, source, base
                    )
                    entry = self._validated_entry(
                        project,
                        entry.collection_id,
                        item,
                        metadata.get("time"),
                        recovery,
                    )
            transaction.put_item(entry)
            transaction.set_checkpoint(
                KafkaCheckpoint(source.topic, source.partition, source.offset + 1)
            )
        return result

    def _validated_entry(
        self, project, collection_id, item, publication_time, recovery
    ):
        if not isinstance(item, dict) or item.get("type") != "Feature":
            raise ValueError("Publication requires a STAC Feature item")
        for field in ("properties", "assets"):
            if not isinstance(item.get(field), dict):
                raise ValueError(f"STAC item {field} must be an object")
        if "links" in item and not isinstance(item["links"], list):
            raise ValueError("STAC item links must be a list")
        checked = {"data": {"payload": {"collection_id": collection_id, "item": item}}}
        if self.plugin_name_for(checked) != project:
            raise ValueError("Patched item must preserve project identity")
        item_id = item.get("id")
        if not isinstance(item_id, str) or not item_id:
            raise ValueError("STAC item requires a non-empty string 'id'")
        plugin = next(plugin for plugin in self.plugins if plugin.name == project)
        dataset_id, version_number = plugin.catalog_index(item_id)
        return CatalogEntry(
            collection_id, item, dataset_id, version_number, publication_time, recovery
        )

    @staticmethod
    def _remote_item(project, collection_id, item_id, source, base=None):
        item, recovery = fetch_current_item(project, collection_id, item_id)
        # Keep the original triggering coordinates across later refreshes.
        if base is not None and base.remote_refresh is not None:
            recovery["trigger"] = base.remote_refresh["trigger"]
        else:
            recovery["trigger"] = {
                "topic": source.topic,
                "partition": source.partition,
                "offset": source.offset,
            }
        LOGGER.warning(
            "Refreshing %s/%s from remote STAC; queued changes are not applied to recovered items",
            collection_id,
            item_id,
        )
        return item, recovery

    def _refresh_recovered_items(self):
        # Partial metadata index avoids scanning/decompressing healthy catalog items.
        for project, catalog in self._catalogs.items():
            with closing(catalog.iter_items(remote_refresh_only=True)) as entries:
                for base in entries:
                    trigger = base.remote_refresh["trigger"]
                    source = KafkaSource(
                        trigger["topic"], trigger["partition"], trigger["offset"]
                    )
                    item, recovery = self._remote_item(
                        project, base.collection_id, base.item_id, source, base
                    )
                    entry = self._validated_entry(
                        project,
                        base.collection_id,
                        item,
                        base.publication_time,
                        recovery,
                    )
                    with catalog.transaction() as transaction:
                        transaction.put_item(entry)

    def close(self) -> None:
        try:
            with ExitStack() as stack:
                for catalog in self._catalogs.values():
                    stack.callback(catalog.close)
        finally:
            self._catalogs.clear()
