"""Read-only status model backed by the local monitoring database."""

from __future__ import annotations

import datetime
import json
import sqlite3
from pathlib import Path


def read_status(
    db_path: str | Path,
    *,
    project: str | None = None,
    stale_after_seconds: float = 15.0,
) -> dict:
    """Return the shared status model used by operator-facing commands."""
    path = Path(db_path)
    if not path.is_file():
        raise FileNotFoundError(f"Monitoring database does not exist: {path}")

    now = datetime.datetime.now(datetime.UTC)
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        try:
            version_row = connection.execute("SELECT value FROM monitor_meta WHERE key = 'schema_version'").fetchone()
            runs = connection.execute("SELECT * FROM monitor_runs ORDER BY heartbeat_at DESC").fetchall()
        except sqlite3.OperationalError as exc:
            raise ValueError(f"{path} is not a valid monitoring database") from exc

        result_runs = []
        for run_row in runs:
            run = dict(run_row)
            project_rows = connection.execute(
                """
                SELECT project, consumed, routed, filtered, succeeded, skipped,
                       failed, handles, updated_at
                FROM monitor_project_stats
                WHERE run_id = ? AND (? IS NULL OR lower(project) = lower(?))
                ORDER BY project
                """,
                (run["run_id"], project, project),
            ).fetchall()
            selected = json.loads(run.pop("selected_projects"))
            if project and project.casefold() not in {value.casefold() for value in selected} and not project_rows:
                continue
            heartbeat = datetime.datetime.fromisoformat(run["heartbeat_at"])
            age = max(0.0, (now - heartbeat).total_seconds())
            state = run["status"]
            if state == "running":
                state = "healthy" if age <= stale_after_seconds else "stale"
            run.update(
                selected_projects=selected,
                state=state,
                heartbeat_age_seconds=age,
                projects=[dict(row) for row in project_rows],
            )
            result_runs.append(run)

    return {
        "schema_version": int(version_row["value"]) if version_row else 0,
        "generated_at": now.isoformat(),
        "stale_after_seconds": stale_after_seconds,
        "runs": result_runs,
    }


__all__ = ["read_status"]
