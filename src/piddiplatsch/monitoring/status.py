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
    include_table_sizes: bool = True,
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
                "SUM(prepared_handles) AS prepared, SUM(errors) AS errors, "
                "SUM(full_events) AS full_events, SUM(patch_events) AS patch_events FROM stats "
                "WHERE kind='batch' AND (? IS NULL OR lower(project)=lower(?)) GROUP BY project "
                "HAVING SUM(items)+SUM(handles)+SUM(errors)+SUM(prepared_handles)>0 ORDER BY project",
                (project, project),
            ).fetchall()
        except sqlite3.Error as exc:
            raise ValueError(f"{path} is not a valid piddi database") from exc
        totals_since = connection.execute(
            "SELECT MIN(mapped_at) FROM stats WHERE kind='batch' "
            "AND (? IS NULL OR lower(project)=lower(?))",
            (project, project),
        ).fetchone()[0]
        from piddiplatsch.monitoring.health import database_health

        health = database_health(connection, project)
        table_sizes = read_table_sizes(connection) if include_table_sizes else None
        payloads = {}
        latest = None
        for row in connection.execute("SELECT id,runtime FROM stats WHERE kind='run'"):
            identity, encoded = row
            fragment = json.loads(encoded)
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
        "event_totals": summarize_event_totals(health["projects"], handle_totals),
        "totals_since": totals_since,
        "table_sizes": table_sizes,
    }


def read_table_sizes(connection):
    """Allocated table and index bytes, across all projects in this snapshot.

    dbstat walks database pages, so interactive callers should sample sparingly.
    Some SQLite builds do not provide the optional dbstat virtual table.
    """
    try:
        rows = connection.execute(
            "SELECT s.tbl_name, "
            "SUM(CASE WHEN s.type='table' THEN d.bytes ELSE 0 END) AS table_bytes, "
            "SUM(CASE WHEN s.type='index' THEN d.bytes ELSE 0 END) AS index_bytes "
            "FROM sqlite_schema AS s JOIN "
            "(SELECT name, SUM(pgsize) AS bytes FROM dbstat GROUP BY name) AS d "
            "ON d.name=s.name WHERE s.type IN ('table','index') "
            "AND s.tbl_name NOT LIKE 'sqlite_%' GROUP BY s.tbl_name "
            "ORDER BY SUM(d.bytes) DESC, s.tbl_name"
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table: dbstat" not in str(exc):
            raise
        return {"available": False, "tables": []}
    return {
        "available": True,
        "tables": [
            dict(
                name=name,
                table_bytes=table_bytes,
                index_bytes=index_bytes,
                total_bytes=table_bytes + index_bytes,
            )
            for name, table_bytes, index_bytes in rows
        ],
    }


def summarize_event_totals(projects, batch_totals):
    """Count accepted source events, including retained failures and cleaned work.

    A successful mapping is counted once in batch stats. Pending/invalid events
    have not mapped successfully yet, including events explicitly retried.
    """
    mapped = _known_sum([row["mapped_items"] for row in batch_totals])
    pending = _known_sum([row.get("pending_items") for row in projects])
    invalid = _known_sum([row.get("invalid_items") for row in projects])
    return dict(
        mapped=mapped,
        pending=pending,
        invalid=invalid,
        total=_known_sum([mapped, pending, invalid]),
    )


def _known_sum(values):
    return None if None in values else sum(values)


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
        "full_events",
        "patch_events",
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
        deltas = {
            counter: last.get(counter, 0) - first.get(counter, 0)
            for counter in counters
        }
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


__all__ = ["read_status", "summarize_event_totals"]
