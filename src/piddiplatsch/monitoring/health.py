"""Read-only backlog checks from the shared database."""

import json
import sqlite3
from contextlib import closing

from piddiplatsch.config import config


def database_health(connection, project=None):
    """Read processing health from the caller's database snapshot."""
    names = {
        row[0]
        for row in connection.execute(
            "SELECT project FROM catalog_items UNION SELECT project FROM work_items "
            "UNION SELECT project FROM stats WHERE project<>''"
        )
    }
    if project:
        names = {project.casefold()}
    projects = []
    for name in sorted(names):
        total, pending, invalid = connection.execute(
            "SELECT (SELECT COUNT(*) FROM catalog_items WHERE project=?), "
            "(SELECT COUNT(*) FROM work_items WHERE project=? AND state='pending'), "
            "(SELECT COUNT(*) FROM work_items WHERE project=? AND state='invalid')",
            (name, name, name),
        ).fetchone()
        batches = connection.execute(
            "SELECT batch_id,SUM(handle_count),MAX(message IS NOT NULL) FROM work_items "
            "WHERE project=? AND state='mapped' GROUP BY batch_id ORDER BY MIN(id)",
            (name,),
        ).fetchall()
        publications = [
            json.loads(row[0])
            for row in connection.execute(
                "SELECT last_publication FROM stats WHERE project=? AND last_publication IS NOT NULL",
                (name,),
            )
        ]
        projects.append(
            dict(
                project=name,
                total_items=total,
                pending_items=pending,
                invalid_items=invalid,
                ready_batches=len(batches),
                failed_batches=sum(row[2] for row in batches),
                ready_handles=sum(row[1] for row in batches),
                ready_batch_ids=[row[0] for row in batches],
                last_publication=max(
                    publications, key=lambda row: row["published_at"], default=None
                ),
                errors=[],
            )
        )
    return {"storage": [], "projects": projects}


def read_health(project: str | None = None) -> dict:
    from piddiplatsch.catalog.api import get_catalog_directory

    database = get_catalog_directory() / "piddi.db"
    try:
        with closing(
            sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)
        ) as connection:
            connection.execute("BEGIN")
            return database_health(connection, project)
    except sqlite3.Error as exc:
        names = (
            [project.casefold()] if project else config.get("consumer", "projects", [])
        )
        return dict(
            storage=[],
            projects=[
                dict(
                    project=name,
                    total_items=None,
                    pending_items=None,
                    invalid_items=None,
                    ready_batches=0,
                    failed_batches=0,
                    last_publication=None,
                    ready_handles=0,
                    ready_batch_ids=[],
                    errors=[f"Catalog status unavailable: {exc}"],
                )
                for name in sorted(names)
            ],
        )
