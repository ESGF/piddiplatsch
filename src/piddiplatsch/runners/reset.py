"""Requeue project work in place and retire its previous output."""

import os
import shutil
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from piddiplatsch.catalog.ownership import ensure_no_work_rebuild, harvest_lock
from piddiplatsch.core.registry import get_plugin
from piddiplatsch.output_maintenance import output_maintenance_lock
from piddiplatsch.runners.reset_selection import select_reset
from piddiplatsch.runners.sync import _save_manifest, _sync_directory, _worker_lock


def processing_paths(settings, project):
    output = Path(settings.get("consumer", "output_dir", "outputs")).resolve()
    database = (
        Path(settings.get("catalog", "directory") or output).resolve() / "piddi.db"
    )
    batches = Path(settings.get("batch", "directory") or output / "batches").resolve()
    return output, database, batches, database


def preview_reset(settings, project, *, from_date=None):
    project = get_plugin(project).name
    output, database, batches, _ = processing_paths(settings, project)
    if not database.is_file():
        return None
    with closing(
        sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
    ) as connection:
        connection.execute("BEGIN")
        return select_reset(connection, project, output, batches, from_date)


def _clear_outbox(connection, project, batch_ids):
    if not connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='outbox_batches'"
    ).fetchone():
        return  # A reset can also operate on a not-yet-upgraded version 4 database.
    if batch_ids is None:
        connection.execute("DELETE FROM handle_outbox WHERE project=?", (project,))
        connection.execute("DELETE FROM outbox_batches WHERE project=?", (project,))
    else:
        for identity in batch_ids:
            connection.execute(
                "DELETE FROM handle_outbox WHERE project=? AND batch_id=?",
                (project, identity),
            )
            connection.execute(
                "DELETE FROM outbox_batches WHERE project=? AND batch_id=?",
                (project, identity),
            )


def reset_processing(settings, project, *, from_date=None):
    """Replay all retained work or an ordered suffix, including complete batches."""
    project = get_plugin(project).name
    output, database, batches, _ = processing_paths(settings, project)
    if not database.is_file():
        raise ValueError(f"Database not found: {database}")
    for source in (batches / project, output / project):
        if (
            source == database.parent
            or source in database.parents
            or source == output / "reset-archives"
        ):
            raise ValueError(f"Cannot archive protected path {source}")
    with (
        harvest_lock(database.parent) as harvest_acquired,
        _worker_lock(database.parent) as acquired,
        output_maintenance_lock(output) as output_acquired,
    ):
        if not harvest_acquired:
            raise RuntimeError("Harvest is running; stop it before reset")
        if not acquired:
            raise RuntimeError("A batch worker is running; stop it before reset")
        if not output_acquired:
            raise RuntimeError("Output maintenance is running; stop it before reset")
        ensure_no_work_rebuild(database.parent)
        moved = []
        with closing(sqlite3.connect(database, isolation_level=None)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                selection = select_reset(
                    connection, project, output, batches, from_date
                )
                if from_date is not None and not selection.count:
                    connection.execute("ROLLBACK")
                    return 0, None
                archive = (
                    output
                    / "reset-archives"
                    / (datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex)
                )
                archive.mkdir(parents=True)
                metadata = dict(
                    project=project,
                    work_entries=selection.count,
                    requested_from=selection.requested_from,
                    effective_from=selection.effective_from,
                    first_work_id=selection.first_id,
                    batch_ids=selection.batch_ids,
                )
                _save_manifest(
                    archive / "reset.json", dict(metadata, status="prepared")
                )
                for source, relative in selection.moves:
                    archived = archive / relative
                    archived.parent.mkdir(parents=True, exist_ok=True)
                    source.rename(archived)
                    moved.append((source, archived))
                    _sync_directory(source.parent)
                    _sync_directory(archived.parent)
                for source, relative in selection.copies:
                    archived = archive / relative
                    archived.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, archived)
                    with archived.open("rb") as stream:
                        os.fsync(stream.fileno())
                    _sync_directory(archived.parent)
                sql = "UPDATE work_items SET state='pending',batch_id=NULL,message=NULL,handle_count=0,updated_at=? WHERE project=?"
                parameters = [datetime.now(UTC).isoformat(), project]
                if from_date is not None:
                    sql += " AND id>=?"
                    parameters.append(selection.first_id)
                connection.execute(sql, parameters)
                _clear_outbox(
                    connection,
                    project,
                    None if from_date is None else selection.batch_ids,
                )
                if from_date is None:
                    connection.execute("DELETE FROM stats WHERE project=?", (project,))
                else:
                    connection.executemany(
                        "DELETE FROM stats WHERE kind='batch' AND project=? AND id=?",
                        [(project, identity) for identity in selection.batch_ids],
                    )
                    connection.execute(
                        "DELETE FROM stats WHERE kind='run' AND project=? AND updated_at>=?",
                        (project, selection.effective_from),
                    )
                connection.execute("COMMIT")
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                for source, archived in reversed(moved):
                    archived.rename(source)
                raise
        _save_manifest(archive / "reset.json", dict(metadata, status="completed"))
    return selection.count, archive
