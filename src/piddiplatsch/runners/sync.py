"""One synchronization pass: seal due project batches and publish ready batches in order."""

import hashlib
import json
import logging
import os
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from piddiplatsch.catalog.api import (
    get_catalog_directory,
    get_catalog_path,
    open_catalog,
)
from piddiplatsch.catalog.state import HarvestState
from piddiplatsch.config import config
from piddiplatsch.config.schema import BatchConfig
from piddiplatsch.core.pipeline import (
    build_processing_target,
    configured_projects,
    process_messages,
)
from piddiplatsch.core.registry import get_plugins
from piddiplatsch.exceptions import BatchError, CatalogError
from piddiplatsch.monitoring.progress import get_progress
from piddiplatsch.monitoring.stats import stats
from piddiplatsch.runners.publish import HandlePublisher

LOGGER = logging.getLogger(__name__)


def _sync_directory(path: Path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _save_manifest(path: Path, manifest: dict):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(manifest, stream, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    _sync_directory(path.parent)


@contextmanager
def _worker_lock(root: Path):
    # Local embedded catalogs and systemd deployment run on POSIX hosts.
    import fcntl

    root.mkdir(parents=True, exist_ok=True)
    _sync_directory(root.parent)
    with (root / ".worker.lock").open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def _fingerprint(path: Path, *, sync=False) -> tuple[str, int]:
    checksum = hashlib.sha256()
    count = 0
    with path.open("rb") as stream:
        for line in stream:
            checksum.update(line)
            count += 1
        if sync:
            os.fsync(stream.fileno())
    return checksum.hexdigest(), count


def _envelope(entry):
    properties = entry.item.get("properties")
    properties = properties if isinstance(properties, dict) else {}
    return (
        entry.item_id,
        {
            "data": {
                "payload": {
                    "method": "POST",
                    "collection_id": entry.collection_id,
                    "item": entry.item,
                }
            },
            "metadata": {
                "time": entry.publication_time
                or properties.get("updated")
                or properties.get("created")
            },
        },
    )


class SyncWorker:
    """Single-process coordinator; immutable payloads and crash-recoverable progress.

    Mapping acknowledgement happens only after durable sealing. A crash between
    seal and acknowledgement recovers from the manifest. Failed publication stays
    ready and blocks newer work for its project, preventing stale overwrite replay.
    """

    def __init__(
        self,
        *,
        now=None,
        publisher=None,
        progress=None,
        projects=(),
        workers=None,
        retries=None,
    ):
        settings = dict(config.get("batch", {}))
        if workers is not None:
            settings["publish_workers"] = workers
        if retries is not None:
            settings["publish_retries"] = retries
        settings = BatchConfig.model_validate(settings).model_dump()
        configured = get_plugins(configured_projects())
        self.plugins = get_plugins(projects) if projects else configured
        configured_names = {plugin.name for plugin in configured}
        for plugin in self.plugins:
            if plugin.name not in configured_names:
                raise ValueError(
                    f"Project {plugin.name!r} is not enabled in [consumer].projects"
                )
        self.root = Path(
            settings.get("directory")
            or Path(config.get("consumer", "output_dir", "outputs")) / "batches"
        ).resolve()
        self.catalog_directory = get_catalog_directory()
        self.trigger_items = settings.get("trigger_items", 1000)
        self.max_wait_seconds = settings.get("max_wait_seconds", 3600)
        self.max_items = settings.get("max_items", 1000)
        self.publish_workers = settings.get("publish_workers", 4)
        self.publish_retries = settings.get("publish_retries", 3)
        self.now = now or (lambda: datetime.now(UTC))
        self.publisher = publisher or HandlePublisher()
        self.errors = []
        self.progress = progress
        self.horizons = {}

    def run(self, *, force_map=False) -> list[Path]:
        completed = []
        self.errors = []
        self.horizons = {}
        with _worker_lock(self.root) as acquired:
            if not acquired:
                LOGGER.info("[sync] Skipped | another worker holds the lock")
                return completed
            LOGGER.info(
                "[sync] Starting | projects: %s",
                ", ".join(p.name for p in self.plugins),
            )
            if HarvestState(self.catalog_directory).pending_clean:
                raise CatalogError(
                    "Catalog clean is incomplete; run harvest before batch processing"
                )
            plugins = self.plugins
            blocked = set()
            while True:
                made_batch = False
                for plugin in plugins:
                    if plugin.name in blocked:
                        continue
                    project_root = self.root / plugin.name
                    try:
                        self._publish_ready(plugin.name, project_root)
                        if not get_catalog_path(
                            plugin.name, directory=self.catalog_directory
                        ).is_file():
                            continue
                        batch = self._map_due(plugin.name, project_root, force_map)
                        if batch is not None:
                            made_batch = True
                            completed.append(batch)
                            self._publish_ready(plugin.name, project_root)
                            if not batch.exists():
                                completed[-1] = project_root / "published" / batch.name
                    except Exception as exc:
                        blocked.add(plugin.name)
                        self.errors.append(f"{plugin.name}: {exc}")
                        LOGGER.error(
                            "[sync] %s | batch processing failed: %s", plugin.name, exc
                        )
                if not made_batch:
                    break
        LOGGER.info(
            "[sync] Complete | %d batches sealed | %d project errors",
            len(completed),
            len(self.errors),
        )
        return completed

    def _map_due(self, project, project_root, force):
        with open_catalog(
            project, directory=self.catalog_directory, read_only=True
        ) as reader:
            frontier = self.horizons.setdefault(
                project, reader.pending_changes().sequence
            )
            pending = reader.pending_changes(max_sequence=frontier)
            if not pending.count:
                return None
            waited = (self.now() - pending.oldest_at).total_seconds()
            if (
                not force
                and pending.count < self.trigger_items
                and waited < self.max_wait_seconds
            ):
                return None
            project_root.mkdir(parents=True, exist_ok=True)
            _sync_directory(self.root)
            batch_id = self.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex
            staging = project_root / (".building-" + batch_id)
            staging.mkdir()
            _sync_directory(project_root)
            manifest = {
                "schema_version": 1,
                "batch_id": batch_id,
                "project": project,
                "created_at": self.now().isoformat(),
                "status": "building",
                "generation": pending.generation,
                "sequence": 0,
                "upper_sequence": frontier,
                "mapping_acknowledged": False,
                "version_history": config.get("catalog", "version_history", False),
                "handle_profile": config.get_handle_profile(project),
                "item_count": 0,
                "handle_count": 0,
            }
            _save_manifest(staging / "manifest.json", manifest)
            try:
                self._map_items(project, reader, staging, manifest)
                manifest["sha256"], manifest["handle_count"] = _fingerprint(
                    staging / "handles.jsonl", sync=True
                )
                (staging / "handles.jsonl").chmod(0o440)
                manifest["status"] = "ready"
                _save_manifest(staging / "manifest.json", manifest)
                ready = project_root / "ready"
                ready.mkdir(exist_ok=True)
                _sync_directory(project_root)
                final = ready / batch_id
                staging.rename(final)
                _sync_directory(ready)
                _sync_directory(project_root)
            except BaseException as exc:
                if staging.exists():
                    manifest["status"] = "failed"
                    manifest["error"] = str(exc)
                    _save_manifest(staging / "manifest.json", manifest)
                raise
        # The read snapshot is closed before opening the write acknowledgement.
        self._acknowledge(final, manifest)
        return final

    def _map_items(self, project, reader, staging, manifest):
        settings = config.get("stats", {})
        LOGGER.info(
            "[sync/map] %s | starting batch %s | force_dataset_pid=%s",
            project,
            manifest["batch_id"],
            config.get_plugin(project, "force_dataset_pid", False),
        )
        stats.configure_for_run(
            enable_db=settings.get("enable_db", False),
            db_path=settings.get("db_path"),
            command="sync",
            selected_projects=(project,),
        )
        if self.progress is not None:
            self.progress.set_phase(f"sync {project} map", mode="map")
        try:
            self._write_items(project, reader, staging, manifest)
        except BaseException as exc:
            stats.close(status="failed", error_summary=str(exc))
            raise
        else:
            stats.close(status="completed")
            summary = stats.summary()
            LOGGER.info(
                "[sync/map] %s | complete | %s items mapped | %s Handles prepared | %s alias Handles",
                project,
                f"{manifest['item_count']:,}",
                f"{summary['handles']:,}",
                f"{summary['alias_handles']:,}",
            )

    def _write_items(self, project, reader, staging, manifest):
        output = staging / "handles.jsonl"
        output.touch()
        target = build_processing_target(
            projects=(project,),
            catalogs={project: reader},
            version_history=manifest["version_history"],
            handle_output_filename=str(output),
            require_all_assets=True,
        )

        def records():
            with closing(
                reader.iter_pending(
                    self.max_items, max_sequence=manifest["upper_sequence"]
                )
            ) as entries:
                for pending in entries:
                    manifest["sequence"] = pending.sequence
                    manifest["item_count"] += 1
                    yield _envelope(pending.entry)

        with closing(records()) as selected:
            result = process_messages(
                selected,
                processor=target,
                max_errors=config.get("consumer", "max_errors", 1),
                progress=self.progress,
            )
        if (
            result.failed
            or result.skipped
            or result.filtered
            or result.succeeded != manifest["item_count"]
        ):
            raise BatchError(
                "Batch mapping did not complete successfully; catalog changes remain pending"
            )

    def _acknowledge(self, batch, manifest):
        if not get_catalog_path(
            manifest["project"], directory=self.catalog_directory
        ).is_file():
            raise CatalogError(
                "Batch source catalog is missing; restore it before publication"
            )
        with open_catalog(
            manifest["project"], directory=self.catalog_directory, read_only=True
        ) as reader:
            if reader.pending_changes().generation != manifest["generation"]:
                raise CatalogError(
                    "Batch belongs to a previous catalog generation; inspect before publication"
                )
        if manifest["mapping_acknowledged"]:
            return
        with open_catalog(
            manifest["project"], directory=self.catalog_directory
        ) as catalog:
            catalog.acknowledge_mapped(manifest["generation"], manifest["sequence"])
        manifest["mapping_acknowledged"] = True
        _save_manifest(batch / "manifest.json", manifest)

    def _publish_ready(self, project, project_root):
        for batch in sorted((project_root / "ready").glob("*")):
            if not batch.is_dir() or batch.name.startswith("."):
                continue
            manifest_path = batch / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            if manifest["project"] != project or manifest.get("schema_version") != 1:
                raise ValueError(f"Invalid batch manifest: {manifest_path}")
            if manifest["status"] == "published":
                self._archive(batch)
                continue
            if manifest["status"] != "ready":
                raise ValueError(f"Unexpected batch status: {manifest_path}")
            output = batch / "handles.jsonl"
            if _fingerprint(output) != (manifest["sha256"], manifest["handle_count"]):
                raise ValueError(f"Batch checksum/count mismatch: {batch}")
            self._acknowledge(batch, manifest)
            manifest["publication_attempts"] = (
                manifest.get("publication_attempts", 0) + 1
            )
            _save_manifest(manifest_path, manifest)
            if self.progress is not None:
                self.progress.close()
            with get_progress(
                title=f"sync {project} publish",
                stream=False,
                unit="handle",
                use_tqdm=self.progress is not None and self.progress.enabled,
            ) as progress:

                def show_progress(
                    index,
                    total,
                    handle,
                    error,
                    tracker=progress,
                    size=manifest["handle_count"],
                ):
                    tracker.update(total=size, position=index, ok=error is None)

                result = self.publisher.run(
                    (output,),
                    project=project,
                    handle_profile=manifest["handle_profile"],
                    log_operation="sync/publish",
                    workers=self.publish_workers,
                    retries=self.publish_retries,
                    progress_callback=show_progress,
                )
            manifest["receipt"] = (
                str(result.result_file) if result.result_file else None
            )
            if result.result_file:
                with result.result_file.open("rb") as stream:
                    os.fsync(stream.fileno())
                _sync_directory(result.result_file.parent)
            if result.failed or result.succeeded != manifest["handle_count"]:
                manifest["publication_error"] = (
                    f"Published {result.succeeded}/{manifest['handle_count']}; failed {result.failed}"
                )
                _save_manifest(manifest_path, manifest)
                raise BatchError(f"Publication failed for {batch}; batch remains ready")
            manifest["status"] = "published"
            manifest["published_at"] = self.now().isoformat()
            manifest.pop("publication_error", None)
            _save_manifest(manifest_path, manifest)
            self._archive(batch)

    @staticmethod
    def _archive(batch):
        published = batch.parent.parent / "published"
        published.mkdir(exist_ok=True)
        _sync_directory(published.parent)
        batch.rename(published / batch.name)
        _sync_directory(batch.parent)
        _sync_directory(published)
