"""Requeue one existing catalog; run with harvest and sync stopped."""

import json
import sqlite3
from contextlib import ExitStack, closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from piddiplatsch.core.registry import get_plugin
from piddiplatsch.runners.sync import _worker_lock


def processing_paths(settings, project):
    output = Path(settings.get("consumer", "output_dir", "outputs")).resolve()
    catalogs = Path(
        settings.get("catalog", "directory") or output / "catalog"
    ).resolve()
    batches = Path(settings.get("batch", "directory") or output / "batches").resolve()
    stats = Path(settings.get("stats", "db_path", "piddi.db")).resolve()
    return output, catalogs / f"{project}.sqlite", batches, stats


def _validate_archive_paths(output, catalog, batches, statistics, project):
    for source in (batches / project, output / project):
        for protected in (
            catalog,
            statistics,
            output / "reset-backups",
            catalog.parent,
        ):
            if source == protected or source in protected.parents:
                raise ValueError(
                    f"Cannot archive {source}: it contains protected path {protected}"
                )


def _clear_project_statistics(statistics, project):
    for table in ("monitor_project_stats", "monitor_samples"):
        statistics.execute(
            f"DELETE FROM {table} WHERE lower(project) = ?",  # noqa: S608 - fixed table names
            (project,),
        )
    # Preserve shared run history. Remove only empty project-only runs.
    for run_id, selected in statistics.execute(
        "SELECT run_id, selected_projects FROM monitor_runs"
    ).fetchall():
        if {name.casefold() for name in json.loads(selected)} == {project}:
            remaining = statistics.execute(
                "SELECT 1 FROM monitor_project_stats WHERE run_id = ? "
                "UNION ALL SELECT 1 FROM monitor_samples WHERE run_id = ? LIMIT 1",
                (run_id, run_id),
            ).fetchone()
            if remaining is None:
                statistics.execute(
                    "DELETE FROM monitor_runs WHERE run_id = ?", (run_id,)
                )


def _open_statistics(stack, stats_path, catalog_path):
    statistics = None
    if stats_path.is_file():
        if stats_path == catalog_path:
            raise ValueError("Monitoring database must be separate from the catalog")
        statistics = stack.enter_context(
            closing(
                sqlite3.connect(
                    stats_path.as_uri() + "?mode=rw", uri=True, isolation_level=None
                )
            )
        )
        for table in ("monitor_runs", "monitor_project_stats", "monitor_samples"):
            statistics.execute(f"SELECT * FROM {table} LIMIT 0")  # noqa: S608
    return statistics


def reset_processing(settings, project):
    """Preserve source records/checkpoints and back up all changed data."""
    project = get_plugin(project).name
    output, catalog_path, batches, stats_path = processing_paths(settings, project)
    _validate_archive_paths(output, catalog_path, batches, stats_path, project)
    if not catalog_path.is_file():
        raise ValueError(f"Catalog not found: {catalog_path}")
    state = catalog_path.parent / ".harvest-state.json"
    if state.is_file() and json.loads(state.read_text()).get("pending_clean"):
        raise ValueError(
            "Catalog clean is incomplete; finish it before resetting processing"
        )
    with _worker_lock(batches) as acquired, ExitStack() as stack:
        if not acquired:
            raise RuntimeError("Sync is running; stop it before resetting processing")
        catalog = stack.enter_context(
            closing(
                sqlite3.connect(
                    catalog_path.as_uri() + "?mode=rw", uri=True, isolation_level=None
                )
            )
        )
        # Validate the schema before moving files or changing data.
        count = catalog.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        catalog.execute(
            "SELECT generation, sequence FROM mapping_state WHERE singleton = 1"
        ).fetchone()
        catalog.execute(
            "SELECT collection_id, item_id, sequence, pending_since FROM pending_items LIMIT 0"
        )
        statistics = _open_statistics(stack, stats_path, catalog_path)
        backup = (
            output
            / "reset-backups"
            / (
                datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
                + f"-{project}-{uuid4().hex[:8]}"
            )
        )
        backup.mkdir(parents=True)
        for connection, filename in (
            (catalog, "catalog.sqlite"),
            (statistics, "monitor.sqlite"),
        ):
            if connection is not None:
                with closing(sqlite3.connect(backup / filename)) as destination:
                    connection.backup(destination)
        (backup / "reset.json").write_text(
            json.dumps(
                {
                    "project": project,
                    "catalog": str(catalog_path),
                    "stats": str(stats_path),
                    "batches": str(batches / project),
                    "project_output": str(output / project),
                },
                indent=2,
            )
            + "\n"
        )
        moved = []
        try:
            catalog.execute("BEGIN IMMEDIATE")
            if statistics is not None:
                statistics.execute("BEGIN IMMEDIATE")
            for source, name in (
                (batches / project, "batches"),
                (output / project, "project-output"),
            ):
                if source.exists():
                    source.rename(backup / name)
                    moved.append((source, backup / name))
            catalog.execute("DELETE FROM pending_items")
            catalog.execute(
                "INSERT INTO pending_items "
                "SELECT collection_id, item_id, ROW_NUMBER() OVER (ORDER BY collection_id, item_id), ? FROM items",
                (datetime.now(UTC).isoformat(),),
            )
            catalog.execute(
                "UPDATE mapping_state SET generation = ?, sequence = ? WHERE singleton = 1",
                (uuid4().hex, count),
            )
            if statistics is not None:
                _clear_project_statistics(statistics, project)
            # Separate SQLite files cannot commit atomically together. Backups
            # remain available if the host stops between these two commits.
            if statistics is not None:
                statistics.execute("COMMIT")
            catalog.execute("COMMIT")
        except BaseException:
            for connection in (catalog, statistics):
                if connection is not None and connection.in_transaction:
                    connection.execute("ROLLBACK")
            for source, archived in reversed(moved):
                archived.rename(source)
            print(f"Reset failed. Keep services stopped; backups: {backup}")
            raise
    return count, backup
