"""Remove disposable completed outbox records without changing work history."""

import sqlite3
from contextlib import closing

from piddiplatsch.catalog.ownership import ensure_no_work_rebuild, harvest_lock
from piddiplatsch.monitoring.disk import get_disk_guard
from piddiplatsch.output_maintenance import output_maintenance_lock
from piddiplatsch.runners.reset import processing_paths
from piddiplatsch.runners.sync import _worker_lock


def clean_outbox(settings, *, vacuum=False):
    output, database, batches, _ = processing_paths(settings, None)
    if not database.is_file():
        raise ValueError(f"Database not found: {database}")
    with (
        harvest_lock(database.parent) as harvest_owned,
        _worker_lock(database.parent) as worker_owned,
        output_maintenance_lock(output) as output_owned,
    ):
        if not (harvest_owned and worker_owned and output_owned):
            raise RuntimeError(
                "Stop harvest, batch workers and maintenance before cleaning"
            )
        ensure_no_work_rebuild(database.parent)
        with closing(sqlite3.connect(database, isolation_level=None)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                version = connection.execute("PRAGMA user_version").fetchone()[0]
                if version not in (4, 5):
                    raise ValueError("Unsupported piddi database schema")
                counts = dict(outbox_batches=0, handle_outbox=0)
                if version == 5:
                    connection.execute(
                        "CREATE TEMP TABLE clean_batches AS "
                        "SELECT project,batch_id FROM outbox_batches AS b "
                        "WHERE status='published' AND NOT EXISTS "
                        "(SELECT 1 FROM handle_outbox AS h WHERE h.project=b.project "
                        "AND h.batch_id=b.batch_id AND h.published=0)"
                    )
                    # Retain the durable completion marker until sync has retired
                    # any stale ready export; otherwise it could be imported again.
                    for path in batches.glob("*/ready/*"):
                        if path.is_dir():
                            connection.execute(
                                "DELETE FROM clean_batches WHERE project=? AND batch_id=?",
                                (path.parent.parent.name, path.name),
                            )
                    for table in ("handle_outbox", "outbox_batches"):
                        sql = {
                            "handle_outbox": "DELETE FROM handle_outbox WHERE (project,batch_id) IN (SELECT project,batch_id FROM clean_batches)",
                            "outbox_batches": "DELETE FROM outbox_batches WHERE (project,batch_id) IN (SELECT project,batch_id FROM clean_batches)",
                        }[table]
                        counts[table] = connection.execute(sql).rowcount
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            if vacuum:
                get_disk_guard(extra_paths=(database.parent,)).check(force=True)
                if connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0]:
                    raise RuntimeError(
                        "Outbox cleanup committed; close database readers and rerun "
                        "piddi admin clean --apply --vacuum to reclaim file space"
                    )
                connection.execute("VACUUM")
                if connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0]:
                    raise RuntimeError(
                        "Outbox cleanup committed; close database readers and rerun "
                        "piddi admin clean --apply --vacuum to finish checkpointing"
                    )
            return counts
