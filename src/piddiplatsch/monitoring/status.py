"""Read-only status model backed by the local monitoring database."""

from __future__ import annotations

import datetime
import json
import sqlite3
from itertools import pairwise
from pathlib import Path


def read_status(
    db_path: str | Path,
    *,
    project: str | None = None,
    stale_after_seconds: float = 15.0,
    history_seconds: float = 3600.0,
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
        version = int(version_row["value"]) if version_row else 0
        if version != 2:
            raise ValueError(f"{path} uses unsupported monitoring schema version {version}")

        result_runs = []
        history_cutoff = (now - datetime.timedelta(seconds=history_seconds)).isoformat()
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
            sample_rows = connection.execute(
                """
                SELECT project, sampled_at, consumed, routed, filtered,
                       succeeded, skipped, failed, handles
                FROM monitor_samples
                WHERE run_id = ? AND sampled_at >= ?
                  AND (? IS NULL OR lower(project) = lower(?))
                ORDER BY project, sampled_at
                """,
                (run["run_id"], history_cutoff, project, project),
            ).fetchall()
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
                history=_summarize_history(sample_rows),
            )
            result_runs.append(run)

    return {
        "schema_version": version,
        "generated_at": now.isoformat(),
        "stale_after_seconds": stale_after_seconds,
        "history_seconds": history_seconds,
        "runs": result_runs,
    }


def _summarize_history(rows: list[sqlite3.Row]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        sample = dict(row)
        grouped.setdefault(sample["project"], []).append(sample)

    summaries = []
    counters = ("consumed", "routed", "filtered", "succeeded", "skipped", "failed", "handles")
    for project, samples in grouped.items():
        first = samples[0]
        last = samples[-1]
        elapsed = max(
            0.0,
            (
                datetime.datetime.fromisoformat(last["sampled_at"])
                - datetime.datetime.fromisoformat(first["sampled_at"])
            ).total_seconds(),
        )
        deltas = {counter: last[counter] - first[counter] for counter in counters}
        rate_series = []
        for previous, current in pairwise(samples):
            seconds = (
                datetime.datetime.fromisoformat(current["sampled_at"])
                - datetime.datetime.fromisoformat(previous["sampled_at"])
            ).total_seconds()
            if seconds > 0:
                rate_series.append((current["consumed"] - previous["consumed"]) / seconds)
        summaries.append(
            {
                "project": project,
                "from": first["sampled_at"],
                "to": last["sampled_at"],
                "sample_count": len(samples),
                "elapsed_seconds": elapsed,
                "deltas": deltas,
                "message_rate": deltas["consumed"] / elapsed if elapsed else 0.0,
                "handle_rate": deltas["handles"] / elapsed if elapsed else 0.0,
                "message_rate_series": rate_series,
            }
        )
    return summaries


__all__ = ["read_status"]
