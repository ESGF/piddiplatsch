"""Manual, archived replacement of event history by current catalog snapshots."""

import base64
import gzip
import json
import os
import re
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from piddiplatsch.catalog.ownership import harvest_lock
from piddiplatsch.monitoring.disk import get_disk_guard
from piddiplatsch.output_maintenance import output_maintenance_lock
from piddiplatsch.runners.reset import processing_paths
from piddiplatsch.runners.sync import _save_manifest, _sync_directory, _worker_lock


def _export(connection, table, destination, guard):
    # The table names below are fixed by callers. Keep the compressed snapshots
    # byte-for-byte; do not inflate/decode and recompress millions of STAC items.
    cursor = connection.execute(
        {"work_items": "SELECT * FROM work_items", "stats": "SELECT * FROM stats"}[
            table
        ]
    )
    columns = [column[0] for column in cursor.description]
    temporary = destination.with_suffix(destination.suffix + ".partial")
    with temporary.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=1) as compressed:
            for index, row in enumerate(cursor):
                if index % 1000 == 0:
                    guard.check()
                record = dict(zip(columns, row, strict=True))
                if "stac_blob" in record:
                    record["stac_blob"] = base64.b64encode(record["stac_blob"]).decode(
                        "ascii"
                    )
                compressed.write(
                    (json.dumps(record, separators=(",", ":")) + "\n").encode()
                )
        raw.flush()
        os.fsync(raw.fileno())
    temporary.replace(destination)
    _sync_directory(destination.parent)


def _sources(output, database, batches, projects):
    candidates = [batches]
    for project in projects:
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", project):
            raise ValueError(f"Unsafe stored project name: {project!r}")
        candidates.append(output / project)
    roots = []
    archive_root = output / "work-rebuild-archives"
    for source in candidates:
        if (
            source == database.parent
            or source in database.parents
            or source == archive_root
            or source in archive_root.parents
        ):
            raise ValueError(f"Cannot archive protected path {source}")
        if source.exists() and not source.is_dir():
            raise ValueError(f"Expected output directory: {source}")
        if source.exists():
            roots.append(source)
    return sorted(
        {
            path
            for path in roots
            if not any(other != path and other in path.parents for other in roots)
        }
    )


def _move(source, destination):
    source.rename(destination)
    _sync_directory(source.parent)
    _sync_directory(destination.parent)


def _restore(plan):
    for index, source in reversed(list(enumerate(plan["sources"]))):
        source = Path(source)
        archived = Path(plan["archive"]) / f"output-{index}"
        if archived.exists():
            if source.exists():
                raise RuntimeError(
                    f"Cannot restore interrupted rebuild: both {source} and {archived} exist"
                )
            _move(archived, source)
        elif not source.exists():
            raise RuntimeError(
                f"Cannot restore interrupted rebuild: missing both {source} and {archived}"
            )


def _committed(connection, identity):
    return (
        connection.execute(
            "SELECT 1 FROM stats WHERE kind='batch' AND project='' AND id=? AND status='work_rebuilt'",
            (identity,),
        ).fetchone()
        is not None
    )


def _finish(connection, journal, plan, guard):
    if plan["vacuum"]:
        guard.check(force=True)
        if connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0]:
            raise RuntimeError(
                "Work was rebuilt; VACUUM cannot run while another connection holds a WAL snapshot. "
                "Close readers and retry piddi admin rebuild --apply."
            )
        connection.execute("VACUUM")
        if connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0]:
            raise RuntimeError(
                "Work was rebuilt; close database readers and retry piddi admin rebuild --apply to finish VACUUM checkpointing"
            )
    _save_manifest(
        Path(plan["archive"]) / "rebuild.json", dict(plan, status="completed")
    )
    journal.unlink()
    _sync_directory(journal.parent)
    return plan


def _require_completed_work(connection):
    unfinished = connection.execute(
        "SELECT state, COUNT(*) FROM work_items INDEXED BY work_selection "
        "WHERE state<>'published' GROUP BY state"
    ).fetchall()
    if unfinished:
        counts = ", ".join(f"{state}={count:,}" for state, count in unfinished)
        raise ValueError(
            f"Cannot rebuild work while unfinished events remain ({counts}). "
            "Finish publication and resolve invalid events first."
        )
    batch = connection.execute(
        "SELECT project,id,status FROM stats WHERE kind='batch' "
        "AND status IN ('ready','publication_failed') LIMIT 1"
    ).fetchone()
    if batch:
        raise ValueError(
            f"Cannot rebuild work while batch {batch[0]}/{batch[1]} is {batch[2]}"
        )


def _recover(connection, journal, output, batches, guard):
    if journal.exists():
        plan = json.loads(journal.read_text())
        if (
            not re.fullmatch(r"[0-9a-f]{32}", plan["id"])
            or Path(plan["archive"]) != output / "work-rebuild-archives" / plan["id"]
        ):
            raise ValueError(
                "Invalid work rebuild journal; inspect it before continuing"
            )
        # Source paths must remain within the original configured roots.
        allowed = {
            batches,
            *(
                output / name
                for name in plan["projects"]
                if re.fullmatch(r"[a-z0-9][a-z0-9_-]*", name)
            ),
        }
        if any(Path(source) not in allowed for source in plan["sources"]):
            raise ValueError(
                "Work rebuild configuration differs from the interrupted run"
            )
        if _committed(connection, plan["id"]):
            _require_completed_work(connection)
            return _finish(connection, journal, plan, guard)
        _restore(plan)
        _save_manifest(
            Path(plan["archive"]) / "rebuild.json",
            dict(plan, status="rolled_back"),
        )
        journal.unlink()
        _sync_directory(journal.parent)
    return None


def rebuild_work(settings, *, vacuum=False):
    """Keep stopped services stopped until this command has completed."""
    output, database, batches, _ = processing_paths(settings, None)
    if not database.is_file():
        raise ValueError(f"Database not found: {database}")
    output.mkdir(parents=True, exist_ok=True)
    guard = get_disk_guard(extra_paths=(database.parent, output))
    with (
        harvest_lock(database.parent) as harvest_owned,
        _worker_lock(database.parent) as worker_owned,
        output_maintenance_lock(output) as output_owned,
    ):
        if not harvest_owned:
            raise RuntimeError("Harvest is running; stop it before rebuilding work")
        if not worker_owned:
            raise RuntimeError(
                "A batch worker is running; stop it before rebuilding work"
            )
        if not output_owned:
            raise RuntimeError(
                "Output maintenance is running; stop it before rebuilding work"
            )
        guard.check(force=True)
        journal = database.parent / ".work-rebuild.json"
        with closing(sqlite3.connect(database, isolation_level=None)) as connection:
            recovered = _recover(connection, journal, output, batches, guard)
            if recovered is not None:
                return recovered
            connection.execute("BEGIN IMMEDIATE")
            plan = None
            try:
                if connection.execute("PRAGMA user_version").fetchone()[0] not in (
                    4,
                    5,
                ):
                    raise ValueError("Unsupported piddi database schema")
                _require_completed_work(connection)
                count = connection.execute(
                    "SELECT COUNT(*) FROM catalog_items"
                ).fetchone()[0]
                if not count:
                    raise ValueError(
                        "Catalog is empty; refusing to replace retained work without catalog snapshots"
                    )
                old_count = connection.execute(
                    "SELECT COUNT(*) FROM work_items"
                ).fetchone()[0]
                projects = [
                    row[0]
                    for row in connection.execute(
                        "SELECT project FROM catalog_items UNION SELECT project FROM work_items UNION SELECT project FROM stats WHERE project<>''"
                    )
                ]
                sources = _sources(output, database, batches, projects)
                identity = uuid4().hex
                archive = output / "work-rebuild-archives" / identity
                archive.mkdir(parents=True)
                plan = dict(
                    id=identity,
                    archive=str(archive),
                    projects=projects,
                    sources=[str(path) for path in sources],
                    catalog_entries=count,
                    previous_work_entries=old_count,
                    vacuum=vacuum,
                    stac_blob_encoding="base64",
                    order="last_update, project, collection_id, item_id",
                    state="published",
                )
                _save_manifest(archive / "rebuild.json", dict(plan, status="exporting"))
                _export(
                    connection, "work_items", archive / "work_items.jsonl.gz", guard
                )
                _export(connection, "stats", archive / "stats.jsonl.gz", guard)
                _save_manifest(journal, plan)
                for index, source in enumerate(sources):
                    _move(source, archive / f"output-{index}")
                connection.execute("DELETE FROM work_items")
                now = datetime.now(UTC).isoformat()
                connection.execute(
                    "INSERT INTO work_items(project,topic,partition,offset,stac_blob,event_type,state,created_at,updated_at) "
                    "SELECT project,?,0,ROW_NUMBER() OVER (ORDER BY last_update,project,collection_id,item_id)-1,"
                    "stac_blob,'full','published',last_update,? FROM catalog_items ORDER BY last_update,project,collection_id,item_id",
                    ("piddi-catalog-rebuild:" + identity, now),
                )
                # A durable receipt disambiguates a crash around COMMIT from a
                # rollback. Zero counters keep it out of operator batch totals.
                connection.execute(
                    "INSERT INTO stats(kind,project,id,status,created_at,updated_at) VALUES('batch','',?,'work_rebuilt',?,?)",
                    (identity, now, now),
                )
                connection.execute("COMMIT")
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                if plan is not None and not _committed(connection, plan["id"]):
                    _restore(plan)
                    _save_manifest(
                        Path(plan["archive"]) / "rebuild.json",
                        dict(plan, status="rolled_back"),
                    )
                    if journal.exists():
                        journal.unlink()
                        _sync_directory(journal.parent)
                raise
            return _finish(connection, journal, plan, guard)
