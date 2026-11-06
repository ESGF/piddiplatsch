"""Daily processing history from persisted run counters."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

METRICS = ("events", "mapped", "prepared_handles", "published_handles", "errors")


def read_insights(
    db_path: str | Path,
    *,
    days: int = 30,
    project: str | None = None,
    now: datetime | None = None,
) -> dict:
    """Aggregate counter deltas by UTC observation day and project.

    Each phase owns its counters: harvest events, map/sync prepared Handles,
    publish successful writes. Batch supervisor rows do not count these again.
    """
    if days < 1:
        raise ValueError("days must be at least 1")
    path = Path(db_path)
    if not path.is_file():
        raise FileNotFoundError(f"Monitoring database does not exist: {path}")
    now = (now or datetime.now(UTC)).astimezone(UTC)
    first = (now.date() - timedelta(days=days - 1)).isoformat()
    last = now.date().isoformat()
    buckets = {}
    warnings = set()
    with closing(
        sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    ) as connection:
        try:
            rows = connection.execute(
                "SELECT project,runtime FROM stats WHERE kind='run' "
                "AND (? IS NULL OR lower(project)=lower(?))",
                (project, project),
            )
            for name, encoded in rows:
                payload = json.loads(encoded)
                run = payload["run"]
                command = run["command"]
                if command not in {"harvest", "consume", "map", "sync", "publish"}:
                    continue
                daily = payload.get("daily")
                if daily is None:
                    # Old versions retained only totals and an hour of samples.
                    # Show those totals honestly on the last observation day.
                    day = (
                        datetime.fromisoformat(run["heartbeat_at"])
                        .astimezone(UTC)
                        .date()
                        .isoformat()
                    )
                    daily = [dict(counts, day=day) for counts in payload["projects"]]
                    if first <= day <= last:
                        warnings.add(
                            "Legacy run totals are assigned to their last heartbeat day; daily splits are unavailable."
                        )
                for counts in daily:
                    day = counts["day"]
                    if not first <= day <= last:
                        continue
                    row = buckets.setdefault(
                        (day, name or "unknown"),
                        dict(
                            day=day,
                            project=name or "unknown",
                            **dict.fromkeys(METRICS, 0),
                        ),
                    )
                    if command in {"harvest", "consume"}:
                        row["events"] += counts.get("consumed", 0)
                    if command in {"map", "sync"}:
                        row["mapped"] += counts.get("succeeded", 0)
                        row["prepared_handles"] += counts.get("handles", 0)
                    if command == "publish":
                        row["published_handles"] += counts.get("handles", 0)
                    row["errors"] += counts.get("failed", 0)
        except (sqlite3.Error, ValueError, KeyError, TypeError) as exc:
            raise ValueError(
                f"{path} is not a valid piddi monitoring database"
            ) from exc
    daily = [
        buckets[key]
        for key in sorted(buckets)
        if any(buckets[key][metric] for metric in METRICS)
    ]
    totals = {}
    for row in daily:
        total = totals.setdefault(
            row["project"], dict(project=row["project"], **dict.fromkeys(METRICS, 0))
        )
        for metric in METRICS:
            total[metric] += row[metric]
    return dict(
        schema_version=1,
        generated_at=now.isoformat(),
        from_date=first,
        to_date=last,
        days=days,
        timezone="UTC",
        daily=daily,
        totals=[totals[name] for name in sorted(totals)],
        warnings=sorted(warnings),
    )
