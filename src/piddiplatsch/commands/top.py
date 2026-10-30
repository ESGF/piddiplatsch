"""Terminal monitoring command."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

import click
from rich.console import Console, Group
from rich.table import Table

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
            except (FileNotFoundError, ValueError) as exc:
                raise click.ClickException(str(exc)) from exc

            if self.json_output:
                click.echo(json.dumps(status, indent=2, sort_keys=True))
                return
            if not self.once:
                console.clear()
            console.print(
                Group(
                    self._disk_table(status),
                    self._health_table(status),
                    self._table(status),
                    self._history_table(status),
                )
            )
            if self.once:
                return
            try:
                time.sleep(self.refresh_seconds)
            except KeyboardInterrupt:
                return

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

        active_runs = [run for run in status["runs"] if run["status"] == "running"]
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
        active_runs = [run for run in status["runs"] if run["status"] == "running"]
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


def _input_unit(command: str) -> str:
    if command == "publish":
        return "handles"
    return "events" if command in {"harvest", "consume"} else "items"


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
