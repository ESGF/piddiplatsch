"""Deduplicate closed Kafka dumps without changing event order or payloads."""

import fcntl
import gzip
import hashlib
import json
import logging
import os
import re
import sqlite3
import tempfile
from contextlib import closing
from datetime import date
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.helpers import utc_now
from piddiplatsch.maintenance import _sync_directory
from piddiplatsch.monitoring.disk import get_disk_guard

LOGGER = logging.getLogger(__name__)


def _fingerprint(line: bytes) -> bytes | None:
    record = json.loads(line)
    if not isinstance(record, dict):
        return None
    source = record.get("source")
    if not isinstance(source, dict) or "payload" not in record:
        return None
    if not isinstance(source.get("topic"), str) or not source["topic"]:
        return None
    if any(
        type(source.get(key)) is not int or source[key] < 0
        for key in ("partition", "offset")
    ):
        return None
    # Include the entire record, not just its coordinates: conflicting content
    # at the same offset must survive for inspection. JSON formatting may differ.
    canonical = json.dumps(
        record, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).digest()


def _rewrite(path, seen, directory, guard, *, dry_run):
    before = path.stat()
    opener = gzip.open if path.suffix == ".gz" else open
    duplicates = 0
    with (
        tempfile.TemporaryDirectory(
            dir=directory, prefix=".deduplicate-part-"
        ) as scratch,
        opener(path, "rb") as source,
        (Path(scratch) / "replacement").open("w+b") as temporary,
    ):
        output = (
            gzip.GzipFile(fileobj=temporary, mode="wb", filename="", compresslevel=1)
            if path.suffix == ".gz"
            else temporary
        )
        try:
            for number, line in enumerate(source, 1):
                if not dry_run:
                    guard.check()
                if not line.endswith(b"\n"):
                    raise ValueError(
                        f"Incomplete dump record: {path}:{number}; keeping original"
                    )
                try:
                    fingerprint = _fingerprint(line)
                except (ValueError, UnicodeError) as exc:
                    raise ValueError(
                        f"Invalid dump record: {path}:{number}; keeping original"
                    ) from exc
                duplicate = (
                    fingerprint is not None
                    and seen.execute(
                        "INSERT OR IGNORE INTO seen VALUES (?)", (fingerprint,)
                    ).rowcount
                    == 0
                )
                if duplicate:
                    duplicates += 1
                elif not dry_run:
                    output.write(line)
        finally:
            if output is not temporary:
                output.close()
        if duplicates and not dry_run:
            temporary.flush()
            owner = os.fstat(temporary.fileno())
            if (owner.st_uid, owner.st_gid) != (before.st_uid, before.st_gid):
                os.fchown(temporary.fileno(), before.st_uid, before.st_gid)
            os.fchmod(temporary.fileno(), before.st_mode & 0o777)
            os.utime(temporary.name, ns=(before.st_atime_ns, before.st_mtime_ns))
            os.fsync(temporary.fileno())
            # Coordinate with DumpRecorder so checking and replacing the file
            # cannot race a writer finishing yesterday's file at midnight.
            with (directory / ".dump.lock").open("a") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                after = path.stat()
                if (
                    before.st_dev,
                    before.st_ino,
                    before.st_size,
                    before.st_mtime_ns,
                ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                    raise OSError(
                        f"Dump changed during deduplication; keeping original: {path}"
                    )
                Path(temporary.name).replace(path)
                _sync_directory(directory)
        return duplicates


def deduplicate_closed_dumps(*, dry_run=False) -> int:
    """Keep the first identical coordinate-bearing event across closed parts.

    SQLite bounds memory usage. Each changed file is replaced atomically; an
    interrupted run can safely repeat without a persistent deduplication index.
    """
    directory = Path(config.get("consumer", "output_dir", "outputs")) / "dump"
    if not directory.is_dir():
        return 0
    today = utc_now().date()
    guard = get_disk_guard(extra_paths=(directory,))
    removed = 0
    with (directory / ".maintenance.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            LOGGER.info(
                "[maintenance] Skipped dump deduplication; maintenance is active"
            )
            return 0
        with tempfile.TemporaryDirectory(
            dir=directory, prefix=".deduplicate-"
        ) as scratch:
            with closing(sqlite3.connect(Path(scratch) / "seen.db")) as seen:
                seen.execute(
                    "CREATE TABLE seen (fingerprint BLOB PRIMARY KEY) WITHOUT ROWID"
                )
                paths = sorted(
                    directory.glob("dump_messages_*.jsonl*"),
                    key=lambda p: p.name.removesuffix(".gz"),
                )
                for path in paths:
                    match = re.fullmatch(
                        r"dump_messages_(\d{4}-\d{2}-\d{2})(?:_\d+)?\.jsonl(?:\.gz)?",
                        path.name,
                    )
                    if not match or path.is_symlink() or not path.is_file():
                        continue
                    try:
                        day = date.fromisoformat(match[1])
                    except ValueError:
                        continue
                    if day >= today:
                        continue
                    if not dry_run:
                        guard.check(force=True)
                    duplicates = _rewrite(path, seen, directory, guard, dry_run=dry_run)
                    seen.commit()
                    removed += duplicates
                    if duplicates:
                        LOGGER.info(
                            "[maintenance] %s %s duplicate event(s) in %s",
                            "Would remove" if dry_run else "Removed",
                            duplicates,
                            path,
                        )
    return removed
