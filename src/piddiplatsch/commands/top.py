"""Terminal monitoring command."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
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
    details: bool = False
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
            if self.details:
                view = Group(
                    self._disk_table(status),
                    self._health_table(status),
                    self._publication_table(status),
                    self._table(status),
                    self._history_table(status),
                )
            else:
                view = self._overview(status)
            console.print(view)
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
        for disk in status["health"]["storage"]:
            free = disk.get("free_bytes")
            space = (
                f"{free / 1024**3:.1f} GiB free"
                if free is not None
                else "unknown free space"
            )
            lines.append(Text(f"Disk: {space} · {disk['state']} · {disk['path']}"))
        harvest = [run for run in active if run["command"] == "harvest"]
        if harvest:
            state = (
                "stale"
                if any(run["state"] == "stale" for run in harvest)
                else "running"
            )
            rate = sum(
                row.get("message_rate", 0) or 0
                for run in harvest
                for row in run.get("history", [])
            )
            lines.append(Text(f"Harvest: {state} · {rate:,.1f} events/s"))
        else:
            lines.append(Text("Harvest: inactive"))

        table = Table(box=None, padding=(0, 1))
        for heading in (
            "Project",
            "Pending items",
            "Prepared",
            "Published",
            "Activity",
            "Handles/s",
            "Batch ETA ~",
        ):
            table.add_column(
                heading,
                min_width=12 if heading == "Project" else None,
                no_wrap=heading == "Project",
                justify=(
                    "right"
                    if heading
                    in {"Pending items", "Prepared", "Published", "Handles/s"}
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
                        f"{name}: errors recorded; inspect --details and the log.",
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
            Text(
                "Pending = catalog items; Prepared / Published = recorded Handle totals.",
                style="dim",
            ),
            Text("--details for batches, runs and history", style="dim"),
        )

    @staticmethod
    def _disk_table(status: dict) -> Table:
        table = Table(title="Storage headroom")
        for heading in ("Path", "State", "Free GiB", "Total GiB"):
            table.add_column(heading)
        for row in status["health"]["storage"]:
            table.add_row(
                row["path"],
                row["state"],
                f"{row['free_bytes'] / 1024**3:.1f}" if "free_bytes" in row else "—",
                f"{row['total_bytes'] / 1024**3:.1f}" if "total_bytes" in row else "—",
            )
        return table

    @staticmethod
    def _health_table(status: dict) -> Table:
        table = Table(title="Project health · Recorded Handle totals")
        for heading in (
            "Project",
            "Pending items",
            "Prepared",
            "Published",
            "Ready",
            "Failed",
            "Last publication",
        ):
            table.add_column(heading)
        totals = {row["project"]: row for row in status.get("handle_totals", [])}
        for row in status["health"]["projects"]:
            last = row["last_publication"] or {}
            counts = totals.get(row["project"], {})
            table.add_row(
                row["project"],
                str(row["pending_items"]) if row["pending_items"] is not None else "—",
                (
                    f"{counts['prepared']:,}"
                    if counts.get("prepared") is not None
                    else "—"
                ),
                (
                    f"{counts['published']:,}"
                    if counts.get("published") is not None
                    else "—"
                ),
                str(row["ready_batches"]),
                str(row["failed_batches"]),
                last.get("published_at") or "—",
            )
            for error in row["errors"]:
                table.add_row(
                    row["project"],
                    "status unavailable",
                    "",
                    "",
                    "",
                    "",
                    error,
                    style="yellow",
                )
        return table

    @staticmethod
    def _publication_table(status: dict) -> Table:
        estimate = status["backlog_estimate"]
        table = Table(title="Publication estimates", caption=estimate.get("reason"))
        for heading in (
            "Scope",
            "Remaining Handles",
            "Handles/s",
            "ETA ~",
            "End ~ (UTC)",
        ):
            table.add_column(heading)
        for run in _active_runs(status):
            if run["command"] == "publish":
                names = ",".join(row["project"] for row in run["projects"])
                table.add_row(*_eta_row(f"{names} batch", run["publication"]))
        table.add_row(*_eta_row("Known backlog ~", estimate))
        return table

    @staticmethod
    def _table(status: dict) -> Table:
        table = Table(title=f"Active runs · {status['generated_at']}")
        for heading, justify in (
            ("State", "left"),
            ("Cmd", "left"),
            ("Project", "left"),
            ("Input", "left"),
            ("Writes", "right"),
            ("Fail", "right"),
            ("Handles", "right"),
            ("Beat", "right"),
        ):
            table.add_column(
                heading,
                justify=justify,
                no_wrap=heading in {"State", "Cmd", "Project"},
            )

        active_runs = _active_runs(status)
        for run in active_runs:
            color = {"healthy": "green", "stale": "yellow", "failed": "red"}.get(
                run["state"], "dim"
            )
            projects = run["projects"] or [
                {
                    "project": ",".join(run["selected_projects"]) or "—",
                    "consumed": 0,
                    "routed": 0,
                    "filtered": 0,
                    "succeeded": 0,
                    "skipped": 0,
                    "failed": 0,
                    "handles": 0,
                    "catalog_writes": 0,
                    "catalog_refreshes": 0,
                }
            ]
            for index, project in enumerate(projects):
                table.add_row(
                    f"[{color}]{run['state']}[/{color}]" if index == 0 else "",
                    run["command"] if index == 0 else "",
                    project["project"],
                    f"{project['consumed']} {_input_unit(run['command'])}",
                    (
                        str(project.get("catalog_writes", 0))
                        if run["command"] == "harvest"
                        else "—"
                    ),
                    str(project["failed"]),
                    str(project["handles"]) if run["command"] != "harvest" else "—",
                    f"{run['heartbeat_age_seconds']:.1f}s" if index == 0 else "",
                )
        if not active_runs:
            table.add_row(
                "—", "—", "no active runs", *("—" for _ in range(5)), style="dim"
            )
        return table

    @staticmethod
    def _history_table(status: dict) -> Table:
        minutes = status["history_seconds"] / 60
        active_runs = _active_runs(status)
        displayed_runs = active_runs or status["runs"][:1]
        scope = "Current run history" if active_runs else "Latest run history"
        table = Table(title=f"{scope} · last {minutes:g} minutes", padding=(0, 0))
        for heading, justify in (
            ("Cmd", "left"),
            ("Project", "left"),
            ("ΔInput", "right"),
            ("Input/s", "right"),
            ("ΔWrites", "right"),
            ("ΔFail", "right"),
            ("ΔHandles", "right"),
            ("Handles/s", "right"),
            ("Trend", "left"),
        ):
            table.add_column(
                heading, justify=justify, no_wrap=heading in {"Cmd", "Project"}
            )

        for run in displayed_runs:
            for history in run["history"]:
                deltas = history["deltas"]
                table.add_row(
                    run["command"],
                    history["project"],
                    f"{deltas['consumed']} {_input_unit(run['command'])}",
                    f"{history['message_rate']:.2f}",
                    (
                        str(deltas.get("catalog_writes", 0))
                        if run["command"] == "harvest"
                        else "—"
                    ),
                    str(deltas["failed"]),
                    str(deltas["handles"]) if run["command"] != "harvest" else "—",
                    (
                        f"{history['handle_rate']:.2f}"
                        if run["command"] != "harvest"
                        else "—"
                    ),
                    _sparkline(history["message_rate_series"]),
                )
        if not any(run["history"] for run in displayed_runs):
            table.add_row("—", "no samples in window", *("—" for _ in range(7)))
        return table


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


def _input_unit(command: str) -> str:
    if command == "publish":
        return "handles"
    return "events" if command in {"harvest", "consume"} else "items"


def _eta_row(scope: str, estimate: dict) -> tuple[str, ...]:
    remaining = estimate.get("remaining_handles")
    rate = estimate.get("handles_per_second")
    end = estimate.get("estimated_end")
    return (
        scope,
        f"{remaining:,}" if remaining is not None else "—",
        f"{rate:,.1f}" if rate is not None else "—",
        _eta_duration(estimate.get("eta_seconds")),
        datetime.fromisoformat(end).strftime("%Y-%m-%d %H:%M") if end else "—",
    )


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


def _sparkline(values: list[float]) -> str:
    if not values:
        return "—"
    blocks = "▁▂▃▄▅▆▇█"
    peak = max(values)
    if peak <= 0:
        return blocks[0] * len(values)
    return "".join(
        blocks[min(len(blocks) - 1, round(value / peak * (len(blocks) - 1)))]
        for value in values[-20:]
    )


__all__ = ["TopCommand"]
