"""Explicit project replay from retained work snapshots, with a database backup."""

import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from piddiplatsch.core.registry import get_plugin
from piddiplatsch.runners.sync import _worker_lock


def processing_paths(settings, project):
    output = Path(settings.get("consumer", "output_dir", "outputs")).resolve()
    database = (
        Path(settings.get("catalog", "directory") or output).resolve() / "piddi.db"
    )
    batches = Path(settings.get("batch", "directory") or output / "batches").resolve()
    return output, database, batches, database


def reset_processing(settings, project):
    """Replay every retained event for a project in order; keep harvest stopped."""
    project = get_plugin(project).name
    output, database, batches, _ = processing_paths(settings, project)
    if not database.is_file():
        raise ValueError(f"Database not found: {database}")
    for source in (batches / project, output / project):
        if (
            source == database.parent
            or source in database.parents
            or source == output / "reset-backups"
        ):
            raise ValueError(f"Cannot archive protected path {source}")
    with _worker_lock(database.parent) as acquired:
        if not acquired:
            raise RuntimeError("A batch worker is running; stop it before reset")
        backup = (
            output
            / "reset-backups"
            / (datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex)
        )
        backup.mkdir(parents=True)
        moved = []
        with closing(sqlite3.connect(database, isolation_level=None)) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM work_items WHERE project=?", (project,)
            ).fetchone()[0]
            with closing(sqlite3.connect(backup / "piddi.db")) as destination:
                connection.backup(destination)
            connection.execute("BEGIN IMMEDIATE")
            try:
                for index, source in enumerate((batches / project, output / project)):
                    if source.exists():
                        archived = backup / f"output-{index}"
                        source.rename(archived)
                        moved.append((source, archived))
                connection.execute(
                    "UPDATE work_items SET state='pending',batch_id=NULL,message=NULL,updated_at=? WHERE project=?",
                    (datetime.now(UTC).isoformat(), project),
                )
                connection.execute("DELETE FROM stats WHERE project=?", (project,))
                connection.execute("COMMIT")
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                for source, archived in reversed(moved):
                    archived.rename(source)
                raise
        (backup / "reset.json").write_text(
            json.dumps({"project": project, "work_entries": count})
        )
    return count, backup
