"""Maintain completed output without locking out ongoing harvest/batch workers."""

import fcntl
import json
import logging
import re
import shutil
import sqlite3
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from piddiplatsch.catalog.api import get_catalog_directory
from piddiplatsch.config import config
from piddiplatsch.config.schema import MaintenanceConfig
from piddiplatsch.helpers import utc_now
from piddiplatsch.maintenance import _compress, _sync_directory
from piddiplatsch.monitoring.disk import get_disk_guard

LOGGER = logging.getLogger(__name__)


@dataclass
class OutputMaintenanceResult:
    compressed: list[Path] = field(default_factory=list)
    removed_batches: list[Path] = field(default_factory=list)
    removed_receipts: list[Path] = field(default_factory=list)


@contextmanager
def output_maintenance_lock(output):
    """Serialize output maintenance with itself and project processing reset."""
    with (output / ".output-maintenance.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
        else:
            yield True


def _inside(path, root):
    return not path.is_symlink() and path.resolve().is_relative_to(root.resolve())


def _receipt_key(path):
    return path.with_suffix("") if path.name.endswith(".jsonl.gz") else path


def _protected_receipts(root):
    protected = set()
    for batch in root.glob("*/ready/*"):
        if not batch.is_dir():
            continue
        path = batch / "manifest.json"
        try:
            manifest = json.loads(path.read_text())
            if manifest.get("receipt"):
                protected.add(_receipt_key(Path(manifest["receipt"]).resolve()))
        except (OSError, ValueError, TypeError, AttributeError):
            LOGGER.warning(
                "[maintenance] Cannot inspect ready batch %s; keeping receipts", path
            )
            return None
    return protected


def _completed_batches(root, database):
    if not database.is_file():
        return []
    with closing(
        sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)
    ) as connection:
        published = {
            (project, identity): timestamp
            for project, identity, timestamp in connection.execute(
                "SELECT project,id,published_at FROM stats WHERE kind='batch' AND status='published'"
            )
        }
    batches = []
    for path in root.glob("*/published/*/*/*/*/manifest.json"):
        batch = path.parent
        if (
            batch.name.startswith(".")
            or not _inside(batch, root)
            or not _inside(path, root)
        ):
            continue
        try:
            manifest = json.loads(path.read_text())
            project = batch.relative_to(root).parts[0]
            identity = manifest["batch_id"]
            if (
                manifest["status"] != "published"
                or manifest["project"] != project
                or identity != batch.name
            ):
                continue
            timestamp = published.get((project, identity))
            if timestamp:
                # Completion, rather than the creation-date directory, controls age.
                finished = max(
                    datetime.fromisoformat(timestamp),
                    datetime.fromisoformat(manifest["published_at"]),
                )
                if finished.tzinfo is None:
                    raise ValueError("Publication timestamp has no timezone")
                batches.append((batch, manifest, finished))
        except (OSError, ValueError, KeyError, TypeError):
            LOGGER.warning("[maintenance] Keeping unverified archive %s", batch)
    return batches


def _remove_batch(batch, dry_run):
    if dry_run:
        return
    # Persist a private deletion name before recursive removal, so a crash can
    # resume cleanup even if the manifest was among the files already removed.
    deleting = batch.with_name(".deleting-" + batch.name)
    batch.rename(deleting)
    _sync_directory(batch.parent)
    shutil.rmtree(deleting)
    _sync_directory(batch.parent)


def _resume_deletions(root, result, dry_run):
    for path in root.glob("*/published/*/*/*/.deleting-*"):
        if (
            path.is_dir()
            and _inside(path, root)
            and re.fullmatch(r"\.deleting-\d{8}T\d{6}-[0-9a-f]{32}", path.name)
        ):
            result.removed_batches.append(path)
            if not dry_run:
                shutil.rmtree(path)
                _sync_directory(path.parent)


def _maintain_batch_files(batches, settings, cutoff, guard, result, dry_run):
    for batch, _manifest, finished in batches:
        if cutoff and finished < cutoff:
            result.removed_batches.append(batch)
            _remove_batch(batch, dry_run)
        elif settings.compress_outputs:
            for name in (
                "handles.jsonl",
                "failures.jsonl",
                "publication-progress.jsonl",
            ):
                path = batch / name
                if (
                    path.is_file()
                    and _inside(path, batch)
                    and not path.with_suffix(".jsonl.gz").exists()
                ):
                    result.compressed.append(path.with_suffix(".jsonl.gz"))
                    if not dry_run:
                        guard.check(force=True)
                        _compress(path, guard)


def _receipt_dates(batches, root):
    dates = {}
    for _batch, manifest, finished in batches:
        if manifest.get("receipt"):
            path = _receipt_key(Path(manifest["receipt"]).resolve())
            if _inside(path, root):
                dates[path] = max(dates.get(path, finished), finished)
    return dates


def _maintain_receipt(path, finished, settings, cutoff, guard, result, dry_run):
    # Publication writers hold this lock until close, including across midnight.
    with path.open("rb") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        if cutoff and finished < cutoff:
            result.removed_receipts.append(path)
            if not dry_run:
                path.unlink()
                _sync_directory(path.parent)
        elif (
            settings.compress_outputs
            and path.suffix == ".jsonl"
            and not path.with_suffix(".jsonl.gz").exists()
        ):
            result.compressed.append(path.with_suffix(".jsonl.gz"))
            if not dry_run:
                guard.check(force=True)
                _compress(path, guard)


def _maintain_receipts(
    root, batches, protected, settings, cutoff, guard, result, now, dry_run
):
    if protected is None:
        return
    dates = _receipt_dates(batches, root)
    paths = {
        path
        for path in root.glob("*/*/*/published*handles*.jsonl*")
        if path.name.endswith((".jsonl", ".jsonl.gz"))
    }
    for path in dates:
        paths.update(
            candidate
            for candidate in (path, path.with_suffix(".jsonl.gz"))
            if candidate.exists()
        )
    for path in sorted(paths):
        if (
            not path.is_file()
            or not _inside(path, root)
            or _receipt_key(path.resolve()) in protected
        ):
            continue
        stat = path.stat()
        finished = dates.get(_receipt_key(path.resolve()))
        if finished is None:
            finished = datetime.fromtimestamp(stat.st_mtime, UTC)
            # Standalone receipt names may have been reserved before the writer
            # opened them. Keep empty/current-day files; completed batches prove
            # their own receipt is closed and can be compressed today.
            if not stat.st_size or finished.date() >= now.date():
                continue
        _maintain_receipt(path, finished, settings, cutoff, guard, result, dry_run)
    if not dry_run:
        _update_receipt_links(batches)


def _update_receipt_links(batches):
    from piddiplatsch.runners.sync import _save_manifest

    for batch, manifest, _finished in batches:
        path = batch / "manifest.json"
        receipt = manifest.get("receipt")
        if not path.is_file() or not receipt or str(receipt).endswith(".gz"):
            continue
        original = Path(receipt)
        compressed = original.with_suffix(".jsonl.gz")
        if not original.exists() and compressed.is_file():
            manifest["receipt"] = str(compressed)
            _save_manifest(path, manifest)


def maintain_outputs(*, dry_run=False):
    settings = MaintenanceConfig.model_validate(config.get("maintenance", {}) or {})
    result = OutputMaintenanceResult()
    output = Path(config.get("consumer", "output_dir", "outputs")).resolve()
    if not output.is_dir() or (
        not settings.compress_outputs and not settings.output_retention_days
    ):
        return result
    root = Path(config.get("batch", "directory") or output / "batches").resolve()
    with output_maintenance_lock(output) as acquired:
        if not acquired:
            LOGGER.info(
                "[maintenance] Output maintenance/reset is active; postponing output cleanup"
            )
            return result
        now = utc_now()
        cutoff = (
            now - timedelta(days=settings.output_retention_days)
            if settings.output_retention_days
            else None
        )
        guard = get_disk_guard(extra_paths=(output, root))
        if cutoff:
            _resume_deletions(root, result, dry_run)
        batches = _completed_batches(root, get_catalog_directory() / "piddi.db")
        protected = _protected_receipts(root)
        _maintain_batch_files(batches, settings, cutoff, guard, result, dry_run)
        _maintain_receipts(
            output / "published",
            batches,
            protected,
            settings,
            cutoff,
            guard,
            result,
            now,
            dry_run,
        )
    return result
