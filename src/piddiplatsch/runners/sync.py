"""One synchronization pass: seal due project batches and publish ready batches in order."""

import hashlib
import json
import logging
import os
import random
from contextlib import ExitStack, closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from piddiplatsch.catalog.api import (
    get_catalog_directory,
    get_catalog_path,
    open_catalog,
)
from piddiplatsch.catalog.outbox import OutboxJournal
from piddiplatsch.catalog.ownership import ensure_no_work_rebuild
from piddiplatsch.catalog.state import HarvestState
from piddiplatsch.config import config
from piddiplatsch.config.schema import BatchConfig
from piddiplatsch.core.pipeline import (
    build_processing_target,
    configured_projects,
    process_messages,
)
from piddiplatsch.core.registry import get_plugins
from piddiplatsch.exceptions import (
    BatchError,
    CatalogError,
    DiskSpaceError,
    MaxErrorsExceededError,
)
from piddiplatsch.monitoring.activity import batch_activity
from piddiplatsch.monitoring.disk import check_disk_space
from piddiplatsch.monitoring.progress import get_progress
from piddiplatsch.monitoring.stats import stats
from piddiplatsch.result import ProcessingResult

LOGGER = logging.getLogger(__name__)


def get_publisher():
    from piddiplatsch.runners.publish import HandlePublisher

    return HandlePublisher()


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


class _BatchMapper:
    """Keep each item's output atomic and retain rejected inputs with the batch."""

    stop_on_error = True

    def __init__(self, target, output, manifest):
        self.target = target
        self.output = output
        self.manifest = manifest

    def plugin_name_for(self, value):
        return self.target.plugin_name_for(value)

    def process(self, key, value):
        offset = self.output.stat().st_size
        try:
            result = self.target.process(key, value)
        except Exception as exc:
            self._rollback(offset)
            # Storage errors can be wrapped by the Handle-writing retry loop.
            cause = exc
            while cause is not None:
                if isinstance(cause, (OSError, CatalogError)):
                    raise
                cause = cause.__cause__ or cause.__context__
            return self._reject(key, value, str(exc))
        except BaseException:
            self._rollback(offset)
            raise
        if not result.success or result.skipped or result.filtered:
            self._rollback(offset)
            return self._reject(
                key,
                value,
                result.error
                or result.skip_reason
                or result.filtered_reason
                or "Mapping did not produce a successful result",
            )
        self.manifest["outcomes"].append(
            {
                "id": self.manifest["last_work_id"],
                "error": None,
                "handle_count": result.num_handles,
                "alias_count": result.num_alias_handles,
            }
        )
        self.manifest["succeeded_count"] += 1
        result.event_type = self.manifest["last_event_type"]
        return result

    def _rollback(self, offset):
        with self.output.open("r+b") as stream:
            stream.truncate(offset)

    def _reject(self, key, value, reason):
        record = {
            **value,
            "__infos__": {
                "project": self.manifest["project"],
                "last_work_id": self.manifest["last_work_id"],
                "reason": reason,
            },
        }
        with (self.output.parent / "failures.jsonl").open(
            "a", encoding="utf-8"
        ) as stream:
            stream.write(json.dumps(record, allow_nan=False) + "\n")
        self.manifest["outcomes"].append(
            {"id": self.manifest["last_work_id"], "error": reason}
        )
        self.manifest["failed_count"] += 1
        LOGGER.error("[sync/map] Rejected %s: %s", key, reason)
        return ProcessingResult(
            key=key,
            project=self.manifest["project"],
            plugin=self.manifest["project"],
            error=reason,
            event_type=self.manifest["last_event_type"],
        )


class SyncWorker:
    """Single-process coordinator; immutable payloads and crash-recoverable progress.

    Mapping payloads, outcomes, and counts commit together in SQLite. Legacy
    ready exports are imported transactionally, including publication progress.
    Failed publication blocks newer work for its project to preserve Handle order.
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
        directory=None,
        handle_profile=None,
        version_history=None,
        require_configured=True,
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
            if require_configured and plugin.name not in configured_names:
                raise ValueError(
                    f"Project {plugin.name!r} is not enabled in [consumer].projects"
                )
        self.root = Path(
            settings.get("directory")
            or Path(config.get("consumer", "output_dir", "outputs")) / "batches"
        ).resolve()
        self.catalog_directory = (
            Path(directory) if directory is not None else get_catalog_directory()
        )
        self.handle_profile = handle_profile
        self.version_history = version_history
        self.from_date = None
        self.to_date = None
        self.remaining = None
        self.offset = 0
        self.map_only = False
        self.trigger_items = settings["trigger_items"]
        self.max_wait_seconds = settings["max_wait_seconds"]
        self.max_items = settings["max_items"]
        self.publish_workers = settings.get("publish_workers", 4)
        self.publish_retries = settings.get("publish_retries", 3)
        self.now = now or (lambda: datetime.now(UTC))
        self.publisher = publisher
        self.errors = []
        self.progress = progress
        self.horizons = {}
        self.skipped_through = {}
        self.mapping_stops = {}
        self.random = random.SystemRandom()
        self._activity = None

    def run(self, *, force_map=False, map_only=False, publish_only=False) -> list[Path]:
        check_disk_space(force=True)
        self.map_only = map_only
        self.publish_only = publish_only
        priorities = self._project_priorities()
        self.published_handles = 0
        completed = []
        self.errors = []
        self.horizons = {}
        self.skipped_through = {}
        self.mapping_stops = {}
        with _worker_lock(self.catalog_directory) as acquired:
            if not acquired:
                LOGGER.info("[sync] Skipped | another worker holds the lock")
                return completed
            ensure_no_work_rebuild(self.catalog_directory)
            self._upgrade_catalog()
            LOGGER.info(
                "[sync] Starting | projects: %s",
                ", ".join(p.name for p in self.plugins),
            )
            if HarvestState(self.catalog_directory).pending_clean:
                raise CatalogError(
                    "Catalog clean is incomplete; run harvest before batch processing"
                )
            blocked = set()
            recovered = set()
            while True:
                plugins = self._select_projects(blocked, priorities, force_map)
                if not plugins:
                    break
                made_progress = False
                for plugin in plugins:
                    project_root = self.root / plugin.name
                    try:
                        with batch_activity(plugin.name) as self._activity:
                            if not map_only or plugin.name not in recovered:
                                published = self._publish_ready(
                                    plugin.name, project_root
                                )
                                recovered.add(plugin.name)
                                if published and not map_only:
                                    made_progress = True
                                    continue
                            if not get_catalog_path(
                                plugin.name, directory=self.catalog_directory
                            ).is_file():
                                continue
                            self._check_mapped_batches(plugin.name, project_root)
                            if publish_only or self.remaining == 0:
                                continue
                            batch = self._map_due(plugin.name, project_root, force_map)
                            if batch is not None:
                                made_progress = True
                                completed.append(batch)
                                completed[-1] = self._finish_mapped_batch(
                                    plugin.name, project_root, batch
                                )
                                if plugin.name in self.mapping_stops:
                                    blocked.add(plugin.name)
                                    reason = self.mapping_stops[plugin.name]
                                    self.errors.append(f"{plugin.name}: {reason}")
                                    LOGGER.warning(
                                        "[sync] %s | mapping paused: %s",
                                        plugin.name,
                                        reason,
                                    )
                    except DiskSpaceError:
                        raise
                    except Exception as exc:
                        blocked.add(plugin.name)
                        self.errors.append(f"{plugin.name}: {exc}")
                        LOGGER.error(
                            "[sync] %s | batch processing failed: %s", plugin.name, exc
                        )
                if not made_progress:
                    # Ready batches may be waiting for older, not-yet-due work.
                    # Stop retrying these candidates while other projects finish.
                    blocked.update(plugin.name for plugin in plugins)
        LOGGER.info(
            "[sync] Complete | %d batches sealed | %d project errors",
            len(completed),
            len(self.errors),
        )
        return completed

    def _set_phase(self, phase):
        if self._activity is not None:
            self._activity.set_phase(phase)

    def _upgrade_catalog(self):
        if (
            self.plugins
            and get_catalog_path(
                self.plugins[0].name, directory=self.catalog_directory
            ).is_file()
        ):
            # Additive schema upgrade; queued legacy files are imported lazily.
            with open_catalog(self.plugins[0].name, directory=self.catalog_directory):
                pass

    def _project_priorities(self):
        if self.map_only or self.publish_only:
            return None
        return {
            plugin.name: config.get_sync_priority(plugin.name)
            for plugin in self.plugins
        }

    def _select_projects(self, blocked, priorities, force):
        active = [plugin for plugin in self.plugins if plugin.name not in blocked]
        if priorities is None:
            return active
        eligible = []
        for plugin in active:
            if not priorities[plugin.name]:
                continue
            try:
                if self._has_sync_work(plugin.name, force):
                    eligible.append(plugin)
            except DiskSpaceError:
                raise
            except Exception as exc:
                blocked.add(plugin.name)
                self.errors.append(f"{plugin.name}: {exc}")
                LOGGER.error(
                    "[sync] %s | batch processing failed: %s", plugin.name, exc
                )
        return (
            self.random.choices(
                eligible, weights=[priorities[p.name] for p in eligible], k=1
            )
            if eligible
            else []
        )

    def _has_sync_work(self, project, force):
        if self._ready_batches(self.root / project):
            return True
        if not get_catalog_path(project, directory=self.catalog_directory).is_file():
            return False
        with open_catalog(
            project, directory=self.catalog_directory, read_only=True
        ) as reader:
            frontier = self._mapping_frontier(project, reader)
            if self._mapping_due(project, reader, frontier, force):
                return True
            # Missing output for mapped work must still be reported as an error.
            return (
                reader._execute(
                    "SELECT 1 FROM work_items WHERE project=? AND state='mapped' LIMIT 1",
                    (project,),
                ).fetchone()
                is not None
            )

    def _mapping_frontier(self, project, reader):
        if project not in self.horizons:
            self.horizons[project] = reader._execute(
                "SELECT COALESCE(MAX(id),0) FROM work_items WHERE project=? AND state='pending'",
                (project,),
            ).fetchone()[0]
        frontier = self.horizons[project]
        mapped = reader._execute(
            "SELECT MIN(id) FROM work_items WHERE project=? AND state='mapped'",
            (project,),
        ).fetchone()[0]
        if (
            not self.map_only
            and mapped is not None
            and reader._execute(
                "SELECT 1 FROM work_items WHERE project=? AND state='pending' AND id<? LIMIT 1",
                (project, mapped),
            ).fetchone()
        ):
            frontier = min(frontier, mapped - 1)
        return frontier

    def _mapping_due(self, project, reader, frontier, force):
        # Only scan the covering index up to the trigger, or one entry when
        # forced. Timestamp reads are bounded to queues below the trigger.
        threshold_met = reader._execute(
            "SELECT 1 FROM work_items WHERE project=? AND state='pending' AND id<=? "
            "LIMIT 1 OFFSET ?",
            (project, frontier, 0 if force else self.trigger_items - 1),
        ).fetchone()
        if threshold_met:
            return True
        if force:
            return False
        oldest = reader._execute(
            "SELECT MIN(created_at) FROM work_items "
            "WHERE project=? AND state='pending' AND id<=?",
            (project, frontier),
        ).fetchone()[0]
        return (
            oldest is not None
            and (self.now() - datetime.fromisoformat(oldest)).total_seconds()
            >= self.max_wait_seconds
        )

    def _finish_mapped_batch(self, project, project_root, batch):
        if not self.map_only:
            self._publish_ready(project, project_root)
        return batch if batch.exists() else self._published_path(batch)

    def _check_mapped_batches(self, project, project_root):
        path = get_catalog_path(project, directory=self.catalog_directory)
        if not path.exists():
            return
        with open_catalog(
            project, directory=self.catalog_directory, read_only=True
        ) as reader:
            batch_ids = [
                row[0]
                for row in reader._execute(
                    # DISTINCT otherwise favors work_batches to avoid sorting,
                    # reading the whole project history just to filter its state.
                    "SELECT DISTINCT batch_id FROM work_items INDEXED BY work_selection "
                    "WHERE project=? AND state='mapped' AND NOT EXISTS "
                    "(SELECT 1 FROM outbox_batches b WHERE b.project=work_items.project AND b.batch_id=work_items.batch_id)",
                    (project,),
                )
            ]
        for batch_id in batch_ids:
            if (
                not batch_id
                or not (project_root / "ready" / batch_id / "manifest.json").is_file()
            ):
                raise BatchError(
                    f"Mapped batch output is missing: {project}/{batch_id}; restore it before continuing"
                )

    def _map_due(self, project, project_root, force):
        self._set_phase("preparing")
        check_disk_space(force=True)
        with open_catalog(
            project, directory=self.catalog_directory, read_only=True
        ) as reader:
            frontier = self._mapping_frontier(project, reader)
            if not self._mapping_due(project, reader, frontier, force):
                return None
            # Select a concrete, bounded batch before invoking its executor.
            if self.offset:
                skipped, last_skipped = reader.skip_work(
                    self.offset,
                    frontier,
                    from_date=self.from_date,
                    to_date=self.to_date,
                )
                self.offset -= skipped
                self.skipped_through[project] = last_skipped
            entries = reader.select_work_ids(
                (
                    min(self.max_items, self.remaining)
                    if self.remaining is not None
                    else self.max_items
                ),
                frontier,
                from_date=self.from_date,
                to_date=self.to_date,
                after_id=self.skipped_through.get(project, 0),
            )
            if not entries:
                return None
            # Filtered manual mapping may already have queued an intervening
            # event. Never put work from both sides into a single newer batch.
            boundary = reader._execute(
                "SELECT MIN(id) FROM work_items WHERE project=? AND state='mapped' AND id>?",
                (project, entries[0]),
            ).fetchone()[0]
            if boundary is not None:
                entries = [identity for identity in entries if identity < boundary]
            project_root.mkdir(parents=True, exist_ok=True)
            _sync_directory(self.root)
            batch_id = self.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex
            staging = project_root / (".building-" + batch_id)
            staging.mkdir()
            _sync_directory(project_root)
            manifest = {
                "schema_version": 3,
                "batch_id": batch_id,
                "project": project,
                "created_at": self.now().isoformat(),
                "status": "building",
                "last_work_id": 0,
                "upper_work_id": frontier,
                "mapping_acknowledged": False,
                "version_history": (
                    self.version_history
                    if self.version_history is not None
                    else config.get("catalog", "version_history", True)
                ),
                "handle_profile": self.handle_profile
                or config.get_handle_profile(project),
                "outcomes": [],
                "item_count": 0,
                "succeeded_count": 0,
                "failed_count": 0,
                "handle_count": 0,
            }
            _save_manifest(staging / "manifest.json", manifest)
            try:
                self._map_items(project, reader, staging, manifest, entries)
                if not manifest["item_count"]:
                    return None
                self._set_phase("sealing")
                manifest["sha256"], manifest["handle_count"] = _fingerprint(
                    staging / "handles.jsonl", sync=True
                )
                manifest["failures_sha256"], failure_count = _fingerprint(
                    staging / "failures.jsonl", sync=True
                )
                if failure_count != manifest["failed_count"]:
                    raise BatchError("Batch failure record count mismatch")
                (staging / "handles.jsonl").chmod(0o440)
                (staging / "failures.jsonl").chmod(0o440)
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
        if manifest.get("mapping_error"):
            self.mapping_stops[project] = (
                manifest["mapping_error"]
                + "; processed items saved; remaining items will continue next sync"
            )
        return final

    def _map_items(self, project, reader, staging, manifest, entries):
        self._set_phase("mapping")
        settings = config.get("stats", {})
        LOGGER.info(
            "[sync/map] %s | starting batch %s | force_dataset_pid=%s",
            project,
            manifest["batch_id"],
            config.get_plugin(project, "force_dataset_pid", False),
        )
        stats.configure_for_run(
            enable_db=settings.get("enable_db", False),
            db_path=None,
            command="map" if self.map_only else "sync",
            selected_projects=(project,),
        )
        if self.progress is not None:
            self.progress.set_phase(f"sync {project} map", mode="map")
        try:
            self._write_items(project, reader, staging, manifest, entries)
        except BaseException as exc:
            stats.close(status="failed", error_summary=str(exc))
            raise
        else:
            stats.close(
                status="failed" if manifest.get("mapping_error") else "completed",
                error_summary=manifest.get("mapping_error"),
            )
            summary = stats.summary()
            LOGGER.info(
                "[sync/map] %s | complete | %s items mapped | %s Handles prepared | %s alias Handles",
                project,
                f"{manifest['item_count']:,}",
                f"{summary['handles']:,}",
                f"{summary['alias_handles']:,}",
            )
            if manifest["failed_count"]:
                LOGGER.warning(
                    "[sync/map] %s | %d rejected items saved in batch failures.jsonl",
                    project,
                    manifest["failed_count"],
                )

    def _write_items(self, project, reader, staging, manifest, entries):
        output = staging / "handles.jsonl"
        output.touch()
        (staging / "failures.jsonl").touch()
        target = build_processing_target(
            projects=(project,),
            catalogs={project: reader},
            version_history=manifest["version_history"],
            handle_output_filename=str(output),
            require_all_assets=True,
            handle_profile=manifest["handle_profile"],
        )

        def records():
            for identity in entries:
                pending = reader.get_work(identity)
                if self.remaining is not None:
                    self.remaining -= 1
                manifest["last_work_id"] = pending.id
                manifest["last_event_type"] = pending.event_type
                manifest["item_count"] += 1
                yield _envelope(pending.entry)

        started = perf_counter()
        try:
            with closing(records()) as selected:
                process_messages(
                    selected,
                    processor=_BatchMapper(target, output, manifest),
                    max_errors=config.get_mapping_max_errors(project),
                    progress=self.progress,
                )
        except MaxErrorsExceededError as exc:
            manifest["mapping_error"] = str(exc)
        finally:
            elapsed = perf_counter() - started
            lookup = target.processors[project].version_lookup
            if lookup is not None and lookup.previous_queries:
                LOGGER.info(
                    "[sync/map] %s | batch %s | version lookups: %s | "
                    "older version found: %s | none: %s | errors: %s | "
                    "%.3fs total | %.3fms avg | %.1f%% of mapping time",
                    project,
                    manifest["batch_id"],
                    f"{lookup.previous_queries:,}",
                    f"{lookup.previous_found:,}",
                    f"{lookup.previous_missing:,}",
                    f"{lookup.previous_queries - lookup.previous_found - lookup.previous_missing:,}",
                    lookup.previous_seconds,
                    1000 * lookup.previous_seconds / lookup.previous_queries,
                    100 * lookup.previous_seconds / elapsed if elapsed else 0.0,
                )
        if (
            manifest["succeeded_count"] + manifest["failed_count"]
            != manifest["item_count"]
        ):
            raise BatchError("Batch mapping outcomes do not match selected items")

    def _acknowledge(self, batch, manifest):
        with open_catalog(
            manifest["project"], directory=self.catalog_directory
        ) as catalog:
            if catalog.outbox_manifest(manifest["batch_id"]) is None:
                self._verify_output(batch, manifest)
                # Old ready batches may already have a per-Handle file journal.
                # Import it atomically with the immutable payloads and outcomes.
                successes = {}
                journal = batch / "publication-progress.jsonl"
                if journal.is_file():
                    from piddiplatsch.runners.publish import _PublicationJournal

                    with closing(
                        _PublicationJournal(journal, batch / "handles.jsonl")
                    ) as old:
                        successes = dict(old.successes)
                with (batch / "handles.jsonl").open() as stream:
                    catalog.seal_batch(
                        manifest["batch_id"],
                        manifest["outcomes"],
                        records=(json.loads(line) for line in stream),
                        manifest=manifest,
                        successes=successes,
                    )
        if not manifest["mapping_acknowledged"]:
            manifest["mapping_acknowledged"] = True
        manifest["outbox"] = True
        self._save_export(batch / "manifest.json", manifest)

    def _ready_batches(self, project_root):
        project = project_root.name
        with ExitStack() as stack:
            reader = None
            if get_catalog_path(project, directory=self.catalog_directory).is_file():
                reader = stack.enter_context(
                    open_catalog(
                        project, directory=self.catalog_directory, read_only=True
                    )
                )
            batches = (
                {manifest["batch_id"]: manifest for manifest in reader.ready_outbox()}
                if reader
                else {}
            )
            for batch in (project_root / "ready").glob("*"):
                if (
                    not batch.is_dir()
                    or batch.name.startswith(".")
                    or batch.name in batches
                ):
                    continue
                saved = reader.outbox_manifest(batch.name) if reader else None
                if saved is not None:
                    # A completed DB transaction needs no replay, even when an
                    # interruption left its optional export in the ready folder.
                    self._save_export(batch / "manifest.json", saved)
                    self._archive_export(batch)
                    continue
                batches[batch.name] = json.loads((batch / "manifest.json").read_text())
        ordered = sorted(
            batches.values(),
            key=lambda manifest: (
                min(entry["id"] for entry in manifest["outcomes"]),
                manifest["batch_id"],
            ),
        )
        return [project_root / "ready" / manifest["batch_id"] for manifest in ordered]

    @staticmethod
    def _save_export(path, manifest):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            _save_manifest(path, manifest)
        except OSError:
            LOGGER.warning(
                "[sync] Could not write optional batch export %s; SQLite retains the queue",
                path,
                exc_info=True,
            )

    def _restore_export(self, batch, manifest):
        self._save_export(batch / "manifest.json", manifest)
        if (batch / "handles.jsonl").exists():
            return
        try:
            with closing(
                OutboxJournal(
                    get_catalog_path(
                        manifest["project"], directory=self.catalog_directory
                    ),
                    manifest["project"],
                    manifest["batch_id"],
                )
            ) as journal:
                temporary = batch / "handles.jsonl.tmp"
                with temporary.open("w") as stream:
                    for _, _, record in journal.records():
                        stream.write(json.dumps(record) + "\n")
                temporary.replace(batch / "handles.jsonl")
            (batch / "failures.jsonl").touch(exist_ok=True)
        except OSError:
            LOGGER.warning(
                "[sync] Could not restore optional payload export %s",
                batch,
                exc_info=True,
            )

    def _load_batch_manifest(self, project, batch):
        with open_catalog(
            project, directory=self.catalog_directory, read_only=True
        ) as reader:
            manifest = reader.outbox_manifest(batch.name)
        return (
            manifest
            if manifest is not None
            else json.loads((batch / "manifest.json").read_text())
        )

    def _archive_export(self, batch):
        try:
            self._archive(batch)
        except OSError:
            LOGGER.warning(
                "[sync] Could not archive optional export %s; publication is committed",
                batch,
                exc_info=True,
            )

    @staticmethod
    def _check_batch_order(project, reader, manifest, first_id):
        last_id = max(entry["id"] for entry in manifest["outcomes"])
        if reader._execute(
            "SELECT 1 FROM work_items WHERE project=? AND state='mapped' AND id>? AND id<? AND batch_id<>? LIMIT 1",
            (project, first_id, last_id, manifest["batch_id"]),
        ).fetchone():
            raise BatchError(
                "Mapped batches overlap in work order; refusing to overwrite newer Handle versions"
            )

    def _publish_ready(self, project, project_root):
        for batch in self._ready_batches(project_root):
            manifest_path = batch / "manifest.json"
            manifest = self._load_batch_manifest(project, batch)
            if manifest["project"] != project or manifest.get("schema_version") != 3:
                raise ValueError(f"Invalid batch manifest: {manifest_path}")
            if manifest["status"] == "published":
                self._record_publication(manifest)
                self._archive(batch)
                if not self.map_only:
                    return True
                continue
            self._set_phase("verifying")
            output = self._verify_output(batch, manifest)
            self._acknowledge(batch, manifest)
            if self.map_only:
                continue
            first_id = min(entry["id"] for entry in manifest["outcomes"])
            with open_catalog(
                project, directory=self.catalog_directory, read_only=True
            ) as reader:
                self._check_batch_order(project, reader, manifest, first_id)
                if (
                    manifest["handle_count"]
                    and reader._execute(
                        "SELECT 1 FROM work_items WHERE project=? AND state='pending' AND id<? LIMIT 1",
                        (project, first_id),
                    ).fetchone()
                ):
                    # Explicit map filters can prepare newer work before older
                    # work. Let the scheduler map the older prefix first.
                    if self.publish_only:
                        raise BatchError(
                            "Older work is still pending; map it or run sync before publishing this batch"
                        )
                    return
            manifest["publication_attempts"] = (
                manifest.get("publication_attempts", 0) + 1
            )
            self._save_export(manifest_path, manifest)
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

                if self.publisher is None:
                    self.publisher = get_publisher()
                self._set_phase("publishing")
                with closing(
                    OutboxJournal(
                        get_catalog_path(project, directory=self.catalog_directory),
                        project,
                        manifest["batch_id"],
                    )
                ) as journal:
                    result = self._send_batch(
                        output,
                        manifest,
                        (output,),
                        project=project,
                        handle_profile=self.handle_profile
                        or manifest["handle_profile"],
                        log_operation="sync/publish",
                        expected_handles=manifest["handle_count"],
                        batch_id=manifest["batch_id"],
                        record_source=journal.records(),
                        journal=journal,
                        workers=self.publish_workers,
                        retries=self.publish_retries,
                        progress_callback=show_progress,
                    )
            self._set_phase("finalizing")
            manifest["receipt"] = (
                str(result.result_file) if result.result_file else None
            )
            if result.result_file:
                with result.result_file.open("rb") as stream:
                    os.fsync(stream.fileno())
                # Persist the date directory chain before marking the batch
                # published. Explicit receipt paths outside output_dir retain
                # the previous parent-directory durability check.
                directory = result.result_file.parent.resolve()
                output_dir = Path(
                    config.get("consumer", "output_dir", "outputs")
                ).resolve()
                while True:
                    _sync_directory(directory)
                    if directory == output_dir or output_dir not in directory.parents:
                        break
                    directory = directory.parent
            if result.failed or result.succeeded != manifest["handle_count"]:
                manifest["publication_error"] = (
                    f"Published {result.succeeded}/{manifest['handle_count']}; failed {result.failed}"
                )
                self._record_publication(manifest, error=manifest["publication_error"])
                self._save_export(manifest_path, manifest)
                raise BatchError(f"Publication failed for {batch}; batch remains ready")
            self._record_publication(manifest)
            self.published_handles += manifest["handle_count"]
            manifest["status"] = "published"
            manifest["published_at"] = self.now().isoformat()
            manifest.pop("publication_error", None)
            self._save_export(manifest_path, manifest)
            self._archive_export(batch)
            return True
        return False

    def _send_batch(self, output, manifest, *args, **kwargs):
        try:
            return self.publisher.run(*args, **kwargs)
        except Exception as exc:
            manifest["publication_error"] = str(exc)
            self._record_publication(manifest, error=str(exc))
            self._save_export(output.parent / "manifest.json", manifest)
            raise

    def _verify_output(self, batch, manifest):
        if manifest.get("outbox"):
            self._restore_export(batch, manifest)
            return batch / "handles.jsonl"
        manifest_path = batch / "manifest.json"
        if manifest["status"] != "ready":
            raise ValueError(f"Unexpected batch status: {manifest_path}")
        output = batch / "handles.jsonl"
        if _fingerprint(output) != (manifest["sha256"], manifest["handle_count"]):
            raise ValueError(f"Batch checksum/count mismatch: {batch}")
        if "failures_sha256" in manifest and _fingerprint(batch / "failures.jsonl") != (
            manifest["failures_sha256"],
            manifest["failed_count"],
        ):
            raise ValueError(f"Batch failure checksum/count mismatch: {batch}")
        return output

    def _record_publication(self, manifest, error=None):
        with open_catalog(
            manifest["project"], directory=self.catalog_directory
        ) as catalog:
            catalog.publish_batch(
                manifest["batch_id"],
                manifest["handle_count"],
                error=error,
                manifest=manifest,
            )

    @staticmethod
    def _published_path(batch: Path) -> Path:
        # Batch IDs carry their creation time in UTC. This partition is stable
        # even when publication or crash recovery finishes on a later day.
        day = datetime.strptime(batch.name[:8], "%Y%m%d")
        return (
            batch.parent.parent
            / "published"
            / day.strftime("%Y")
            / day.strftime("%m")
            / day.strftime("%d")
            / batch.name
        )

    @classmethod
    def _archive(cls, batch):
        manifest = json.loads((batch / "manifest.json").read_text())
        # A small durable marker lets top show the latest success without
        # walking the complete publication archive on every refresh.
        _save_manifest(
            batch.parent.parent / "last-publication.json",
            {
                key: manifest.get(key)
                for key in ("batch_id", "published_at", "handle_count", "receipt")
            },
        )
        destination = cls._published_path(batch)
        published = batch.parent.parent / "published"
        # Persist every newly created level before the atomic move, including
        # when a previous crash left part of the directory tree behind.
        for directory in (
            published,
            destination.parent.parent.parent,
            destination.parent.parent,
            destination.parent,
        ):
            directory.mkdir(exist_ok=True)
            _sync_directory(directory.parent)
        batch.rename(destination)
        _sync_directory(batch.parent)
        _sync_directory(destination.parent)
