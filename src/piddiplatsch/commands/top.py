"""Terminal monitoring command."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import click
from rich.console import Console, Group
from rich.table import Table
from rich.text import Text

from piddiplatsch.config import config
from piddiplatsch.monitoring.eta import backlog_estimate
from piddiplatsch.monitoring.rates import WINDOWS
from piddiplatsch.monitoring.sampling import BackgroundMetrics
from piddiplatsch.monitoring.status import read_status, summarize_event_totals

from .base import Command


def _queue_activity(row: dict, generated_at: str | None = None) -> str:
    if (
        row.get("pending_items") or row.get("ready_batches")
    ) and config.get_sync_priority(row["project"]) == 0:
        return "disabled"
    if row.get("ready_batches"):
        return "queued"
    pending = row.get("pending_items")
    if pending is None:
        return "sampling"
    if not pending:
        return "idle"
    if pending >= config.get("batch", "trigger_items", 100):
        return "ready"
    oldest = row.get("pending_oldest_at")
    if oldest is None:
        return "pending"  # An older/partial metrics snapshot cannot prove eligibility.
    now = datetime.fromisoformat(generated_at) if generated_at else datetime.now(UTC)
    age = (now - datetime.fromisoformat(oldest)).total_seconds()
    return "ready" if age >= config.get("batch", "max_wait_seconds", 600) else "waiting"


@dataclass(kw_only=True)
class TopCommand(Command):
    """Render current process and per-project progress from SQLite."""

    db_path: Path
    project: str | None = None
    once: bool = False
    json_output: bool = False
    refresh_seconds: float = 2.0
    stale_after_seconds: float = 15.0

    def execute(self) -> None:
        console = Console()
        live = not (self.once or self.json_output)
        metrics = BackgroundMetrics(self.db_path, self.project) if live else None
        if not self.json_output:
            console.print("Reading database status…", style="dim")
        while True:
            try:
                status = read_status(
                    self.db_path,
                    project=self.project,
                    stale_after_seconds=self.stale_after_seconds,
                    history_seconds=300,
                    include_table_sizes=not live,
                    include_health=not live,
                )
                if metrics is not None:
                    metrics.update(status)
                status["backlog_estimate"] = backlog_estimate(status)
            except (FileNotFoundError, ValueError) as exc:
                raise click.ClickException(str(exc)) from exc

            if self.json_output:
                click.echo(json.dumps(status, indent=2, sort_keys=True))
                return
            if not self.once:
                console.clear()
            console.print(self._overview(status))
            if self.once:
                return
            try:
                time.sleep(self.refresh_seconds)
            except KeyboardInterrupt:
                return

    @staticmethod
    def _overview(status: dict) -> Group:
        """Keep the daily operator view to one table and actionable status lines."""
        active = _active_runs(status)
        lines = [_totals_line(status)]
        lines.append(
            Text(
                f"Batch: {config.get('batch', 'trigger_items', 100):,} events / "
                f"{_eta_duration(config.get('batch', 'max_wait_seconds', 600))} "
                f"· max {config.get('batch', 'max_items', 2000):,}",
                style="dim",
            )
        )
        starts = (
            [status["totals_since"]]
            if status.get("totals_since")
            else [run["started_at"] for run in status["runs"] if run.get("started_at")]
        )
        if starts:
            start = min(datetime.fromisoformat(value) for value in starts).astimezone(
                UTC
            )
            lines.append(
                Text(f"Handle totals: since {start:%Y-%m-%d %H:%M} UTC", style="dim")
            )
        for disk in status["health"]["storage"]:
            free = disk.get("free_bytes")
            space = (
                f"{free / 1024**3:.1f} GiB free"
                if free is not None
                else "unknown free space"
            )
            lines.append(Text(f"Disk: {space} · {disk['state']} · {disk['path']}"))
        lines.extend(_table_size_lines(status.get("table_sizes")))
        lines.extend(_metrics_lines(status.get("metrics")))
        harvest = [run for run in active if run["command"] == "harvest"]
        harvest_rates = _harvest_rates(harvest)
        if harvest:
            state = (
                "stale"
                if any(run["state"] == "stale" for run in harvest)
                else "running"
            )
            rate = f"{sum(harvest_rates.values()):,.1f}" if harvest_rates else "—"
            total = sum(
                row.get("consumed", 0) for run in harvest for row in run["projects"]
            )
            harvest_starts = [
                datetime.fromisoformat(run["started_at"]).astimezone(UTC)
                for run in harvest
                if run.get("started_at")
            ]
            since = (
                f"since {min(harvest_starts):%Y-%m-%d %H:%M} UTC"
                if harvest_starts
                else "since run start"
            )
            lines.append(
                Text(
                    f"Harvest: {state} · {rate} events/s (5m avg) · {total:,} events {since}"
                    + _harvest_event_types(harvest)
                )
            )
        else:
            lines.append(Text("Harvest: inactive"))
        table = Table(box=None, padding=(0, 1))
        for heading in (
            "Project",
            "Catalog items",
            "Pending events",
            "Prepared handles",
            "Published handles",
            "Errors",
            "Activity",
            "Harvest/s",
            "Harvest trend →",
            "Handles/s",
            "Batch progress",
            "Batch ETA ~",
        ):
            table.add_column(
                heading,
                min_width=(
                    12 if heading == "Project" else max(map(len, heading.split()))
                ),
                no_wrap=heading == "Project",
                justify=(
                    "right"
                    if heading
                    in {
                        "Catalog items",
                        "Pending events",
                        "Prepared handles",
                        "Published handles",
                        "Errors",
                        "Harvest/s",
                        "Handles/s",
                    }
                    else "left"
                ),
            )
        health = {row["project"]: row for row in status["health"]["projects"]}
        totals = {row["project"]: row for row in status.get("handle_totals", [])}
        names = set(health) | set(totals)
        error_totals = {}
        for run in status["runs"]:
            names.update(_run_projects(run))
            for project in run["projects"]:
                name = project["project"]
                names.add(name)
                error_totals[name] = error_totals.get(name, 0) + project.get(
                    "failed", 0
                )
        error_totals.update(
            {name: counts.get("errors", 0) for name, counts in totals.items()}
        )
        alerts = []
        for name in sorted(names):
            row = health.get(name, {})
            counts = totals.get(name, {})
            runs = [run for run in active if name in _run_projects(run)]
            publishers = [
                run
                for run in runs
                if run["command"] == "publish" and run["state"] == "healthy"
            ]
            estimate = (
                publishers[0].get("publication", {}) if len(publishers) == 1 else {}
            )
            activity = _queue_activity(row, status.get("generated_at"))
            if any(
                run["command"] in {"sync", "map"} and run["state"] == "healthy"
                for run in runs
            ):
                activity = "mapping"
            phase = _batch_phase(runs)
            if phase is not None:
                activity = phase
            if publishers:
                activity = "publishing"
            if (
                row.get("failed_batches")
                or row.get("invalid_items")
                or _latest_mapping_has_errors(name, status["runs"])
                or _latest_batch_has_errors(name, status["runs"])
                or any(
                    p.get("failed", 0)
                    for run in runs
                    for p in run["projects"]
                    if p["project"] == name
                )
            ):
                activity += " · errors"
                alerts.append(
                    Text(
                        f"{name}: errors recorded; inspect the log or top --json.",
                        style="red",
                    )
                )
            alerts.extend(
                Text(
                    f"{name}: {run['command']} heartbeat is stale ({run['heartbeat_age_seconds']:.0f}s).",
                    style="yellow",
                )
                for run in runs
                if run["state"] == "stale"
            )
            alerts.extend(
                Text(f"{name}: {error}", style="yellow")
                for error in row.get("errors", [])
            )
            eta = "—"
            if estimate.get("eta_seconds") is not None:
                end = datetime.fromisoformat(estimate["estimated_end"])
                eta = f"{_eta_duration(estimate['eta_seconds'])} ({end:%H:%M} UTC)"
            rate = estimate.get("handles_per_second")
            table.add_row(
                name,
                _count(row.get("total_items")),
                _count(row.get("pending_items")),
                _count(counts.get("prepared")),
                _count(counts.get("published")),
                Text(
                    _count(error_totals.get(name)),
                    style="red" if error_totals.get(name) else "",
                ),
                activity,
                f"{harvest_rates[name]:,.1f}" if name in harvest_rates else "—",
                _harvest_trend(harvest, name),
                f"{rate:,.1f}" if rate is not None else "—",
                _batch_progress(publishers),
                eta,
            )
        alerts.extend(_backlog_eta_lines(status.get("backlog_estimate", {})))
        return Group(
            *lines,
            table,
            *_processing_rate_lines(status.get("processing_rates", {})),
            *alerts,
        )


def _processing_rate_lines(rates):
    if not rates:
        return []
    parts = ["Avg ~ 10m/30m/1h"]
    for phase, title, unit in (
        ("harvest", "Harvest", "events/s"),
        ("map", "Map", "events/s"),
        ("publish", "Publish", "handles/s"),
    ):
        values = [rates[phase][label]["per_second"] for label in WINDOWS]
        speeds = "/".join(
            f"{value:,.1f}" if value is not None else "—" for value in values
        )
        parts.append(f"{title} {speeds} {unit}")
    return [Text(" · ".join(parts))]


def _backlog_eta_lines(backlog):
    label = (
        "Overall publish ETA (partial)"
        if backlog.get("partial")
        else "Overall publish ETA"
    )
    if backlog.get("eta_seconds") is not None:
        end = datetime.fromisoformat(backlog["estimated_end"]).astimezone(UTC)
        lines = [
            Text(
                f"{label}: ~{_eta_duration(backlog['eta_seconds'])} ({end:%Y-%m-%d %H:%M} UTC) · current backlog only"
            )
        ]
        if backlog.get("rate_source") in {"recent_batch", "mixed"}:
            lines[0].append(" · using recent batch speed during warm-up")
    else:
        reason = backlog.get("reason") or "waiting for an estimate"
        lines = [Text(f"{label}: unavailable · {reason}", style="dim")]
    missing = []
    for project in backlog.get("unestimated_projects", []):
        pending = project.get("pending_events")
        count = f"{pending:,} pending events; " if pending is not None else ""
        missing.append(f"{project['project']} ({count}{project['reason']})")
    if missing:
        lines.append(Text("Not estimated: " + "; ".join(missing)))
    return lines


def _totals_line(status: dict) -> Text:
    projects = status["health"]["projects"]
    counts = [row.get("total_items") for row in projects]
    items = (
        sum(counts) if counts and all(value is not None for value in counts) else None
    )
    events = status.get("event_totals") or summarize_event_totals(
        projects, status.get("handle_totals", [])
    )
    return Text(
        f"Catalog: {_count(items)} items · Mapped: {_count(events['mapped'])} / {_count(events['total'])} events"
    )


def _harvest_event_types(runs):
    rows = [row for run in runs for row in run["projects"]]
    full = sum(row.get("full_events", 0) for row in rows)
    patches = sum(row.get("patch_events", 0) for row in rows)
    return f" · stored: {full:,} full / {patches:,} patch"


def _harvest_trend(runs: list[dict], project: str) -> str:
    for run in runs:
        for row in run.get("history", []):
            if row["project"] == project:
                return _sparkline(row.get("message_rate_series", []))
    return "—"


def _sparkline(values: list[float], width: int = 12) -> str:
    """Average the whole history into at most twelve bars, oldest first."""
    if not values:
        return "—"
    size = min(width, len(values))
    buckets = [
        values[index * len(values) // size : (index + 1) * len(values) // size]
        for index in range(size)
    ]
    averages = [max(0, sum(bucket) / len(bucket)) for bucket in buckets]
    peak = max(averages)
    blocks = "▁▂▃▄▅▆▇█"
    return "".join(
        blocks[round(value / peak * 7)] if peak else blocks[0] for value in averages
    )


def _batch_progress(publishers: list[dict]) -> str:
    """Successful Handle writes in this publication run, against its batch size."""
    if len(publishers) != 1:
        return "—"
    run = publishers[0]
    published = run.get("publication", {}).get("published_handles")
    if published is None:
        published = sum(row.get("handles", 0) for row in run["projects"])
    total = run.get("expected_handles")
    return f"{published:,} / {total:,}" if total is not None else f"{published:,} / ?"


def _harvest_rates(runs: list[dict]) -> dict[str, float]:
    rates = {}
    for run in runs:
        if run["state"] == "healthy":
            for row in run.get("history", []):
                if row.get("elapsed_seconds", 0) > 0:
                    name = row["project"]
                    rates[name] = rates.get(name, 0) + row["message_rate"]
    return rates


def _count(value: int | None) -> str:
    return f"{value:,}" if value is not None else "—"


def _table_size_lines(sizes):
    if sizes is None:
        return []
    if not sizes["available"]:
        reason = sizes.get("reason", "estimate unavailable")
        return [Text(f"SQLite table sizes: unavailable ({reason})", style="dim")]
    names = {
        "work_items": "work",
        "catalog_items": "catalog",
        "kafka_checkpoints": "checkpoints",
        "handle_outbox": "outbox",
        "outbox_batches": "batches",
    }
    entries = " · ".join(
        f"{names.get(row['name'], row['name'])} {_bytes(row['total_bytes'])}"
        for row in sizes["tables"]
    )
    label = "DB ~" if sizes.get("estimated") else "DB"
    return [Text(f"{label} (all projects, incl. indexes): {entries}")]


def _metrics_lines(metrics):
    if metrics is None:
        return []
    if metrics.get("error"):
        return [Text(f"Database metrics: {metrics['error']}", style="yellow")]
    sampled_at = metrics.get("counts_sampled_at")
    if sampled_at is None:
        return [
            Text(
                "Catalog/backlog counts and table sizes: measuring in background…",
                style="dim",
            )
        ]
    timestamp = datetime.fromisoformat(sampled_at).astimezone(UTC)
    suffix = " · refreshing in background" if metrics["sampling"] else ""
    return [
        Text(f"Database counts: sampled {timestamp:%H:%M:%S} UTC{suffix}", style="dim")
    ]


def _bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            amount = f"{size:.1f}".removesuffix(".0")
            return f"{amount} {unit}"
        size /= 1024


def _run_projects(run: dict) -> set[str]:
    return {row["project"] for row in run.get("projects", [])} or set(
        run.get("selected_projects", [])
    )


def _latest_mapping_has_errors(project: str, runs: list[dict]) -> bool:
    # read_status returns newest heartbeats first. Publication starts a separate
    # run, including for zero Handles; it must not hide the preceding map errors.
    for run in runs:
        if (
            run["command"] not in {"sync", "map"}
            or run.get("status") == "running"
            or project not in _run_projects(run)
        ):
            continue
        return run.get("state") == "failed" or any(
            row.get("failed", 0)
            for row in run.get("projects", [])
            if row["project"] == project
        )
    return False


def _batch_phase(runs: list[dict]) -> str | None:
    batch = next((run for run in runs if run["command"] == "batch"), None)
    if batch is None:
        return None
    phase = batch.get("phase", "preparing")
    return f"{phase} · stale" if batch["state"] == "stale" else phase


def _latest_batch_has_errors(project: str, runs: list[dict]) -> bool:
    latest = next(
        (
            run
            for run in runs
            if run["command"] == "batch"
            and run.get("status") != "running"
            and project in _run_projects(run)
        ),
        None,
    )
    return latest is not None and latest.get("state") == "failed"


def _active_runs(status: dict) -> list[dict]:
    """Hide stale records superseded by a newer run, including a finished one.

    Healthy concurrent runs remain visible. Raw JSON retains every record.
    """
    result = []
    for run in status["runs"]:
        if run.get("status") != "running":
            continue
        projects = _run_projects(run)
        newer_projects = set()
        if run.get("state") == "stale" and run.get("started_at"):
            for other in status["runs"]:
                if (
                    other["command"] == run["command"]
                    and other.get("started_at", "") > run["started_at"]
                ):
                    newer_projects.update(_run_projects(other))
        if projects and projects <= newer_projects:
            continue
        result.append(run)
    return result


def _eta_duration(seconds: float | None) -> str:
    if seconds is None:
        return "waiting"
    minutes, seconds = divmod(int(max(0, seconds)), 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    parts = [
        f"{value}{unit}"
        for value, unit in ((days, "d"), (hours, "h"), (minutes, "m"), (seconds, "s"))
        if value
    ]
    return " ".join(parts[:2]) or "0s"


__all__ = ["TopCommand"]
