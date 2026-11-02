"""Read-only backlog checks from the shared database."""

import sqlite3
from contextlib import closing

from piddiplatsch.config import config


def database_health(connection, project=None, *, fast=False):
    """Read processing health from the caller's database snapshot."""
    names = (
        {project.casefold()}
        if project
        else {row[0] for row in _project_names(connection)}
    )
    projects = []
    for name in sorted(names):
        if fast:
            total = connection.execute(
                "SELECT COUNT(*) FROM catalog_items INDEXED BY catalog_versions WHERE project=?",
                (name,),
            ).fetchone()[0]
            states = dict(
                connection.execute(
                    "SELECT state,COUNT(*) FROM work_items INDEXED BY work_selection "
                    "WHERE project=? GROUP BY state",
                    (name,),
                )
            )
            pending, invalid = states.get("pending", 0), states.get("invalid", 0)
            full = patches = None
        else:
            total, pending, invalid, full, patches = connection.execute(
                "SELECT (SELECT COUNT(*) FROM catalog_items WHERE project=?), "
                "(SELECT COUNT(*) FROM work_items WHERE project=? AND state='pending'), "
                "(SELECT COUNT(*) FROM work_items WHERE project=? AND state='invalid'), "
                "(SELECT COUNT(*) FROM work_items WHERE project=? AND state='pending' AND event_type='full'), "
                "(SELECT COUNT(*) FROM work_items WHERE project=? AND state='pending' AND event_type='patch')",
                (name, name, name, name, name),
            ).fetchone()
        batches = connection.execute(
            "SELECT id,prepared_handles,status='publication_failed' FROM stats "
            "WHERE kind='batch' AND project=? AND status IN ('ready','publication_failed') "
            "ORDER BY mapped_at,id",
            (name,),
        ).fetchall()
        publication = connection.execute(
            "SELECT id,published_at,handles FROM stats WHERE kind='batch' AND project=? "
            "AND status='published' ORDER BY published_at DESC,id DESC LIMIT 1",
            (name,),
        ).fetchone()
        projects.append(
            dict(
                project=name,
                total_items=total,
                pending_items=pending,
                pending_full_events=full,
                pending_patch_events=patches,
                invalid_items=invalid,
                ready_batches=len(batches),
                failed_batches=sum(row[2] for row in batches),
                ready_handles=sum(row[1] for row in batches),
                ready_batch_ids=[row[0] for row in batches],
                last_publication=(
                    dict(
                        batch_id=publication[0],
                        published_at=publication[1],
                        handle_count=publication[2],
                    )
                    if publication
                    else None
                ),
                errors=[],
            )
        )
        if fast:
            projects[-1]["work_items"] = sum(states.values())
    return {"storage": [], "projects": projects}


def _project_names(connection):
    """Seek to the next project in each index instead of visiting every row."""
    names = {
        row[0]
        for row in connection.execute(
            "SELECT DISTINCT project FROM stats WHERE project<>''"
        )
    }
    for table in ("catalog_items", "work_items"):
        previous = None
        while True:
            if previous is None:
                row = connection.execute(
                    f"SELECT project FROM {table} ORDER BY project LIMIT 1"  # noqa: S608 - fixed table allowlist
                ).fetchone()
            else:
                row = connection.execute(
                    f"SELECT project FROM {table} WHERE project>? ORDER BY project LIMIT 1",  # noqa: S608 - fixed table allowlist
                    (previous,),
                ).fetchone()
            if row is None:
                break
            previous = row[0]
            names.add(previous)
    return [(name,) for name in names]


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
                    pending_full_events=None,
                    pending_patch_events=None,
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
