"""Reconstruct project catalogs from publication events without Handle mapping."""

import logging
from collections import defaultdict
from contextlib import ExitStack, closing
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import jsonpatch

from piddiplatsch.catalog.api import get_catalog_path, open_catalog
from piddiplatsch.catalog.base import BaseCatalog, BaseCatalogTransaction
from piddiplatsch.catalog.models import CatalogEntry, KafkaCheckpoint
from piddiplatsch.catalog.ownership import ensure_no_work_rebuild, harvest_lock
from piddiplatsch.catalog.remote import fetch_current_item
from piddiplatsch.catalog.state import HarvestState
from piddiplatsch.config import config
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.core.plugin import normalize_project_id
from piddiplatsch.core.registry import get_plugins
from piddiplatsch.core.routing import extract_project_id
from piddiplatsch.exceptions import CatalogError, InvalidPublicationError
from piddiplatsch.monitoring.disk import get_disk_guard
from piddiplatsch.persist.dump import DumpRecorder
from piddiplatsch.result import ProcessingResult

LOGGER = logging.getLogger(__name__)


@dataclass
class _PatchMetrics:
    attempts: int = 0
    local: int = 0
    remote: int = 0
    seconds: float = 0.0


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
        ignore_unknown_projects: bool | None = None,
    ):
        self.disk_guard = get_disk_guard(extra_paths=(directory,))
        self.disk_guard.check(force=True)
        if backend != "sqlite":
            raise ValueError(f"Unsupported catalog backend: {backend!r}")
        self.directory = Path(directory)
        self.backend = backend
        self.ignore_unknown_projects = (
            config.get("catalog", "ignore_unknown_projects", False)
            if ignore_unknown_projects is None
            else ignore_unknown_projects
        )
        if type(self.ignore_unknown_projects) is not bool:
            raise ValueError("catalog.ignore_unknown_projects must be a boolean")
        self._unknown_recorder = None
        self.plugins = get_plugins("all")
        self._plugins_by_project = {
            project_id: plugin
            for plugin in self.plugins
            for project_id in plugin.project_ids
        }
        self._catalogs: dict[str, BaseCatalog] = {}
        self._patch_metrics: dict[str, _PatchMetrics] = defaultdict(_PatchMetrics)
        self._patches_since_log = 0
        self._patch_log_at = perf_counter()
        self._patch_log_seconds = config.get("stats", "interval_seconds", 30)
        self._patch_log_count = config.get("stats", "summary_interval", 10000)
        self._ownership = ExitStack()
        self._state = HarvestState(self.directory)
        try:
            if not self._ownership.enter_context(harvest_lock(self.directory)):
                raise CatalogError(
                    "Another harvest or work rebuild is running; stop it first"
                )
            ensure_no_work_rebuild(self.directory)
            known_projects = set(self.project_names)
            if set(clean_projects) - known_projects:
                raise ValueError("Clean projects must be registered plugin names")
            path = get_catalog_path(self.project_names[0], directory=self.directory)
            if clean_projects and path.exists():
                catalog = self._get_catalog(self.project_names[0])
                stored = {
                    row[0]
                    for row in catalog._execute(
                        "SELECT DISTINCT project FROM catalog_items UNION SELECT DISTINCT project FROM work_items"
                    )
                }
                if stored - set(clean_projects):
                    raise CatalogError(
                        "Project-only harvest cleaning is unsafe with shared Kafka checkpoints; "
                        "use piddi admin recover --clean-catalog for a complete rebuild"
                    )
            if clean_projects:
                self._state.request_clean(clean_projects)
            if set(self._state.pending_clean) - known_projects:
                raise CatalogError("Pending clean references an unregistered project")
            if self._state.pending_clean and path.exists():
                catalog = self._get_catalog(self.project_names[0])
                with catalog.transaction():
                    stored = {
                        row[0]
                        for row in catalog._execute(
                            "SELECT DISTINCT project FROM catalog_items UNION SELECT DISTINCT project FROM work_items"
                        )
                    }
                    if stored - set(self._state.pending_clean):
                        raise CatalogError(
                            "Pending clean must include every stored project because Kafka checkpoints are shared"
                        )
                    catalog._execute("DELETE FROM catalog_items")
                    catalog._execute("DELETE FROM work_items")
                    catalog._execute("DELETE FROM kafka_checkpoints")
                    catalog._execute("DELETE FROM stats")
                    catalog._execute("DELETE FROM handle_outbox")
                    catalog._execute("DELETE FROM outbox_batches")
            # Open project views for recovered-item refresh without migration.
            if path.exists():
                for project in self.project_names:
                    self._get_catalog(project)
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

    def preflight_check(self) -> None:
        return None

    def kafka_resume_offset(self, topic: str, partition: int) -> int | None:
        """Checkpoints are shared; read fresh progress on each assignment."""
        checkpoint = self._get_catalog(self.project_names[0]).get_checkpoint(
            topic, partition
        )
        return checkpoint.next_offset if checkpoint else None

    def plugin_name_for(self, value: dict) -> str | None:
        project = extract_project_id(value)
        plugin = self._plugins_by_project.get(normalize_project_id(project or ""))
        return plugin.name if plugin else None

    def _get_catalog(self, project: str) -> BaseCatalog:
        if project not in self._catalogs:
            # Plugin names also become spool directories; reject unsafe names.
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
        """Measure patch resolution; item validation and durable storage follow."""
        metrics = self._patch_metrics[project]
        started = perf_counter()
        metrics.attempts += 1
        try:
            item, recovery = self._resolve_patch(project, payload, transaction, source)
            if recovery is None:
                metrics.local += 1
            else:
                metrics.remote += 1
            return item, recovery
        finally:
            metrics.seconds += perf_counter() - started
            self._patches_since_log += 1
            self._log_patch_metrics()

    def _log_patch_metrics(self, *, force=False):
        now = perf_counter()
        if not force and (
            self._patches_since_log < self._patch_log_count
            and now - self._patch_log_at < self._patch_log_seconds
        ):
            return
        for project, metrics in self._patch_metrics.items():
            LOGGER.info(
                "[harvest] %s | patch resolutions: %s | local: %s | "
                "remote STAC: %s | errors: %s | %.3fs total | %.3fms avg",
                project,
                f"{metrics.attempts:,}",
                f"{metrics.local:,}",
                f"{metrics.remote:,}",
                f"{metrics.attempts - metrics.local - metrics.remote:,}",
                metrics.seconds,
                1000 * metrics.seconds / metrics.attempts,
            )
        self._patch_metrics.clear()
        self._patches_since_log = 0
        self._patch_log_at = now

    def _resolve_patch(
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
            raise InvalidPublicationError(
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
        self.disk_guard.check()
        project_id = extract_project_id(value)
        if project_id is None:
            raise InvalidPublicationError("Publication project is missing")
        plugin = self._plugins_by_project.get(normalize_project_id(project_id))
        if plugin is None:
            if self.ignore_unknown_projects:
                return self._ignore_unknown(key, value, source, project_id)
            raise InvalidPublicationError(
                f"Publication project is not registered: {project_id!r}"
            )
        project = plugin.name
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
            transaction.add_work(
                entry, source, event_type="patch" if method == "PATCH" else "full"
            )
            transaction.set_checkpoint(
                KafkaCheckpoint(source.topic, source.partition, source.offset + 1)
            )
        result.event_type = "patch" if method == "PATCH" else "full"
        result.catalog_written = True
        result.catalog_refreshed = recovery is not None
        return result

    def _ignore_unknown(self, key, value, source, project_id):
        # Checkpoints belong to the source partition, not the project view.
        catalog = self._get_catalog(self.project_names[0])
        result = ProcessingResult(
            key=key,
            success=True,
            project=normalize_project_id(project_id),
            filtered=True,
            filtered_reason="Unknown project ignored",
        )
        with catalog.transaction() as transaction:
            checkpoint = transaction.get_checkpoint(source.topic, source.partition)
            if checkpoint is not None and source.offset < checkpoint.next_offset:
                result.filtered_reason = "Kafka message already harvested"
                return result
            if self._unknown_recorder is None:
                self._unknown_recorder = DumpRecorder(prefix="unknown_projects")
            # This dedicated archive is mandatory even when the caller did not
            # request the ordinary all-event dump. It must precede the checkpoint.
            path = self._unknown_recorder.record(
                key,
                {
                    "source": {
                        "topic": source.topic,
                        "partition": source.partition,
                        "offset": source.offset,
                    },
                    "key": key,
                    "payload": value,
                },
            )
            transaction.set_checkpoint(
                KafkaCheckpoint(source.topic, source.partition, source.offset + 1)
            )
        LOGGER.warning(
            "[harvest] Ignored unknown project %r at %s/%d/%d; archived in %s",
            project_id,
            source.topic,
            source.partition,
            source.offset,
            path,
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
                    self.disk_guard.check()
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
                self._log_patch_metrics(force=True)
        finally:
            self._catalogs.clear()
            self._ownership.close()
