"""Read-only status model backed by the local monitoring database."""

from __future__ import annotations

import datetime
import json
import sqlite3
from contextlib import closing
from itertools import pairwise
from pathlib import Path

from piddiplatsch.monitoring.eta import publication_progress


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
    with closing(
        sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    ) as connection:
        connection.execute("BEGIN")
        connection.row_factory = sqlite3.Row
        try:
            handle_totals = connection.execute(
                "SELECT project, SUM(items) AS mapped_items, SUM(handles) AS published, "
                "SUM(prepared_handles) AS prepared, SUM(errors) AS errors FROM stats "
                "WHERE (? IS NULL OR lower(project)=lower(?)) GROUP BY project "
                "HAVING SUM(items)+SUM(handles)+SUM(errors)+SUM(prepared_handles)>0 ORDER BY project",
                (project, project),
            ).fetchall()
        except sqlite3.Error as exc:
            raise ValueError(f"{path} is not a valid piddi database") from exc
        from piddiplatsch.monitoring.health import database_health

        health = database_health(connection, project)
        payloads = {}
        latest = None
        for row in connection.execute("SELECT runtime FROM stats"):
            for identity, fragment in json.loads(row[0]).items():
                if (
                    latest is None
                    or fragment["run"]["heartbeat_at"] > latest["run"]["heartbeat_at"]
                ):
                    latest = fragment
                payload = payloads.setdefault(
                    identity, dict(run=fragment["run"], projects=[], samples=[])
                )
                if fragment["run"]["heartbeat_at"] > payload["run"]["heartbeat_at"]:
                    payload["run"] = fragment["run"]
                payload["projects"].extend(fragment["projects"])
                payload["samples"].extend(fragment["samples"])
        if latest:
            health["storage"] = latest.get("storage", [])
        result_runs = []
        cutoff = (now - datetime.timedelta(seconds=history_seconds)).isoformat()
        for payload in payloads.values():
            try:
                run = payload["run"]
                selected = run.get("selected_projects", [])
                rows = [
                    row
                    for row in payload["projects"]
                    if project is None
                    or row["project"].casefold() == project.casefold()
                ]
                if (
                    project
                    and project.casefold() not in {name.casefold() for name in selected}
                    and not rows
                ):
                    continue
                samples = [
                    row
                    for row in payload["samples"]
                    if row["sampled_at"] >= cutoff
                    and (
                        project is None
                        or row["project"].casefold() == project.casefold()
                    )
                ]
                samples.sort(key=lambda row: row["sampled_at"])
                heartbeat = datetime.datetime.fromisoformat(run["heartbeat_at"])
                age = max(0.0, (now - heartbeat).total_seconds())
                state = run["status"]
                if state == "running":
                    state = "healthy" if age <= stale_after_seconds else "stale"
                run.update(
                    state=state,
                    heartbeat_age_seconds=age,
                    projects=rows,
                    history=_summarize_history(samples),
                )
                if run["command"] == "publish":
                    run["publication"] = publication_progress(run, samples, now)
                result_runs.append(run)
            except (OSError, ValueError, KeyError, TypeError):
                continue
        result_runs.sort(key=lambda run: run["heartbeat_at"], reverse=True)
        version = 1

    return {
        "schema_version": version,
        "generated_at": now.isoformat(),
        "stale_after_seconds": stale_after_seconds,
        "history_seconds": history_seconds,
        "runs": result_runs,
        "health": health,
        "handle_totals": [dict(row) for row in handle_totals],
    }


def _summarize_history(rows: list[sqlite3.Row]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        sample = dict(row)
        grouped.setdefault(sample["project"], []).append(sample)

    summaries = []
    counters = (
        "consumed",
        "routed",
        "filtered",
        "succeeded",
        "skipped",
        "failed",
        "handles",
        "catalog_writes",
        "catalog_refreshes",
    )
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
                rate_series.append(
                    (current["consumed"] - previous["consumed"]) / seconds
                )
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
