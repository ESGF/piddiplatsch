"""Compress closed raw dumps; never modify batch payloads or receipts."""

import fcntl
import gzip
import logging
import os
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
        os.fsync(temporary.fileno())
        after = path.stat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise OSError(f"Dump changed during compression; keeping original: {path}")
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
        for path in sorted(directory.glob("dump_messages_*.jsonl")):
            if path.is_symlink() or not path.is_file():
                continue
            try:
                day = date.fromisoformat(path.stem.removeprefix("dump_messages_"))
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
