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

from piddiplatsch.monitoring.eta import backlog_estimate
from piddiplatsch.monitoring.health import read_health
from piddiplatsch.monitoring.status import read_status

from .base import Command


@dataclass(kw_only=True)
class TopCommand(Command):
    """Render current process and per-project progress from SQLite."""

    db_path: Path
    project: str | None = None
    once: bool = False
    json_output: bool = False
    refresh_seconds: float = 2.0
    stale_after_seconds: float = 15.0
    history_minutes: float = 60.0

    def execute(self) -> None:
        console = Console()
        while True:
            try:
                status = read_status(
                    self.db_path,
                    project=self.project,
                    stale_after_seconds=self.stale_after_seconds,
                    history_seconds=self.history_minutes * 60,
                )
                status["health"] = read_health(self.project)
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
        lines = []
        starts = [run["started_at"] for run in status["runs"] if run.get("started_at")]
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
        harvest = [run for run in active if run["command"] == "harvest"]
        harvest_rates = _harvest_rates(harvest)
        window = _eta_duration(status.get("history_seconds", 3600))
        if harvest:
            state = (
                "stale"
                if any(run["state"] == "stale" for run in harvest)
                else "running"
            )
            rate = f"{sum(harvest_rates.values()):,.1f}" if harvest_rates else "—"
            lines.append(Text(f"Harvest: {state} · {rate} events/s"))
        else:
            lines.append(Text("Harvest: inactive"))
        lines.append(
            Text(
                f"Rate windows: harvest up to {window} · publish up to 60s", style="dim"
            )
        )

        table = Table(box=None, padding=(0, 1))
        for heading in (
            "Project",
            "Pending datasets",
            "Generated handles",
            "Published handles",
            "Activity",
            "Harvest/s",
            "Handles/s",
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
                        "Pending datasets",
                        "Generated handles",
                        "Published handles",
                        "Harvest/s",
                        "Handles/s",
                    }
                    else "left"
                ),
            )
        health = {row["project"]: row for row in status["health"]["projects"]}
        totals = {row["project"]: row for row in status.get("handle_totals", [])}
        names = set(health) | set(totals)
        for run in status["runs"]:
            names.update(row["project"] for row in run["projects"])
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
            activity = "idle"
            if row.get("ready_batches"):
                activity = "queued"
            elif row.get("pending_items"):
                activity = "pending"
            if any(
                run["command"] in {"sync", "map"} and run["state"] == "healthy"
                for run in runs
            ):
                activity = "mapping"
            if publishers:
                activity = "publishing"
            if row.get("failed_batches") or any(
                p.get("failed", 0)
                for run in runs
                for p in run["projects"]
                if p["project"] == name
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
                _count(row.get("pending_items")),
                _count(counts.get("prepared")),
                _count(counts.get("published")),
                activity,
                f"{harvest_rates[name]:,.1f}" if name in harvest_rates else "—",
                f"{rate:,.1f}" if rate is not None else "—",
                eta,
            )
        backlog = status.get("backlog_estimate", {})
        if backlog.get("eta_seconds") is not None:
            alerts.append(
                Text(
                    f"Known backlog: ~{_eta_duration(backlog['eta_seconds'])} to publish (excludes future arrivals)."
                )
            )
        return Group(
            *lines,
            table,
            *alerts,
        )


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


def _run_projects(run: dict) -> set[str]:
    return {row["project"] for row in run.get("projects", [])} or set(
        run.get("selected_projects", [])
    )


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
