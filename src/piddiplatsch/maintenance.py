"""Compress closed raw dumps and maintain retained database work/history."""

import fcntl
import gzip
import json
import logging
import os
import re
import tempfile
from datetime import date
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.helpers import utc_now
from piddiplatsch.monitoring.disk import get_disk_guard

LOGGER = logging.getLogger(__name__)


def _sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _compress(path: Path, guard) -> Path:
    destination = path.with_suffix(".jsonl.gz")
    before = path.stat()
    with (
        path.open("rb") as source,
        tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
        ) as temporary,
    ):
        owner = os.fstat(temporary.fileno())
        if (owner.st_uid, owner.st_gid) != (before.st_uid, before.st_gid):
            os.fchown(temporary.fileno(), before.st_uid, before.st_gid)
        os.fchmod(temporary.fileno(), before.st_mode & 0o777)
        with gzip.GzipFile(
            fileobj=temporary,
            mode="wb",
            filename=path.name,
            compresslevel=1,
            mtime=int(before.st_mtime),
        ) as compressed:
            while chunk := source.read(1024 * 1024):
                guard.check()
                compressed.write(chunk)
        temporary.flush()
        os.utime(temporary.name, ns=(before.st_atime_ns, before.st_mtime_ns))
        os.fsync(temporary.fileno())
        after = path.stat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise OSError(f"File changed during compression; keeping original: {path}")
        # Publish atomically without replacing any existing archive. The
        # original is removed only once the complete archive is durable.
        os.link(temporary.name, destination)
        _sync_directory(path.parent)
        path.unlink()
        _sync_directory(path.parent)
    return destination


def compress_closed_dumps(*, dry_run=False) -> list[Path]:
    directory = Path(config.get("consumer", "output_dir", "outputs")) / "dump"
    if not directory.is_dir():
        return []
    today = utc_now().date()
    guard = get_disk_guard(extra_paths=(directory,))
    completed = []
    with (directory / ".maintenance.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            LOGGER.info("[maintenance] Skipped; another maintenance run holds the lock")
            return []
        archives = (
            *directory.glob("dump_messages_*.jsonl"),
            *directory.glob("unknown_projects_*.jsonl"),
        )
        for path in sorted(archives):
            if path.is_symlink() or not path.is_file():
                continue
            match = re.fullmatch(
                r"(?:dump_messages|unknown_projects)_(\d{4}-\d{2}-\d{2})(?:_\d+)?\.jsonl",
                path.name,
            )
            if match is None:
                continue
            try:
                day = date.fromisoformat(match[1])
            except ValueError:
                continue
            if day >= today or path.with_suffix(".jsonl.gz").exists():
                continue
            if dry_run:
                LOGGER.info("[maintenance] Would compress %s", path)
                completed.append(path)
                continue
            guard.check(force=True)
            destination = _compress(path, guard)
            LOGGER.info("[maintenance] Compressed %s", destination)
            completed.append(destination)
    return completed


def clean_published_work(*, dry_run=False):
    """Delete only expired published work; summaries already committed with state.

    This never compacts the database or removes catalog records and output files.
    """
    import sqlite3
    from contextlib import closing
    from datetime import UTC, datetime, timedelta

    from piddiplatsch.catalog.api import get_catalog_directory
    from piddiplatsch.config.schema import BatchConfig
    from piddiplatsch.runners.sync import _worker_lock

    directory = get_catalog_directory()
    path = directory / "piddi.db"
    if not path.is_file():
        return 0
    settings = BatchConfig.model_validate(config.get("batch", {}))
    if settings.retention_days == 0:
        return 0
    cutoff = (datetime.now(UTC) - timedelta(days=settings.retention_days)).isoformat()
    with _worker_lock(directory) as acquired:
        if not acquired:
            LOGGER.info("[maintenance] Batch worker is active; postponing work cleanup")
            return 0
        with closing(sqlite3.connect(path)) as connection:
            if dry_run:
                return connection.execute(
                    "SELECT COUNT(*) FROM work_items WHERE state='published' AND updated_at<?",
                    (cutoff,),
                ).fetchone()[0]
            with connection:
                return connection.execute(
                    "DELETE FROM work_items WHERE state='published' AND updated_at<?",
                    (cutoff,),
                ).rowcount


def clean_status_history(*, dry_run=False):
    """Prune expired completed run rows without touching retained batch summaries."""
    import sqlite3
    from contextlib import closing
    from datetime import UTC, datetime, timedelta

    from piddiplatsch.catalog.api import get_catalog_directory

    days = config.get("stats", "sample_retention_days", 30)
    database = get_catalog_directory() / "piddi.db"
    if not days or not database.is_file():
        return 0
    cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    count = 0
    with closing(sqlite3.connect(database, timeout=5)) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        for project, identity, encoded in connection.execute(
            "SELECT project,id,runtime FROM stats WHERE kind='run'"
        ).fetchall():
            run = json.loads(encoded)["run"]
            if (
                run.get("status") == "running"
                or not run.get("finished_at")
                or run["finished_at"] >= cutoff
            ):
                continue
            count += 1
            if not dry_run:
                connection.execute(
                    "DELETE FROM stats WHERE kind='run' AND project=? AND id=?",
                    (project, identity),
                )
    return count
