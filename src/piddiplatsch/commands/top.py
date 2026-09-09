"""Terminal monitoring command."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

import click
from rich.console import Console, Group
from rich.table import Table

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
            except (FileNotFoundError, ValueError) as exc:
                raise click.ClickException(str(exc)) from exc

            if self.json_output:
                click.echo(json.dumps(status, indent=2, sort_keys=True))
                return
            if not self.once:
                console.clear()
            console.print(Group(self._table(status), self._history_table(status)))
            if self.once:
                return
            try:
                time.sleep(self.refresh_seconds)
            except KeyboardInterrupt:
                return

    @staticmethod
    def _table(status: dict) -> Table:
        table = Table(title=f"piddiplatsch monitor · {status['generated_at']}")
        for heading, justify in (
            ("State", "left"),
            ("Cmd", "left"),
            ("Project", "left"),
            ("Msg", "right"),
            ("Route", "right"),
            ("Filter", "right"),
            ("OK", "right"),
            ("Skip", "right"),
            ("Fail", "right"),
            ("Hdl", "right"),
            ("Beat", "right"),
        ):
            table.add_column(
                heading,
                justify=justify,
                no_wrap=heading in {"State", "Cmd", "Project"},
            )

        for run in status["runs"]:
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
                }
            ]
            for index, project in enumerate(projects):
                table.add_row(
                    f"[{color}]{run['state']}[/{color}]" if index == 0 else "",
                    run["command"] if index == 0 else "",
                    project["project"],
                    *(
                        str(project[key])
                        for key in (
                            "consumed",
                            "routed",
                            "filtered",
                            "succeeded",
                            "skipped",
                            "failed",
                            "handles",
                        )
                    ),
                    f"{run['heartbeat_age_seconds']:.1f}s" if index == 0 else "",
                )
        if not status["runs"]:
            table.add_row("dim", "—", "no matching runs", *("—" for _ in range(8)))
        return table

    @staticmethod
    def _history_table(status: dict) -> Table:
        minutes = status["history_seconds"] / 60
        table = Table(title=f"History · last {minutes:g} minutes")
        for heading, justify in (
            ("Cmd", "left"),
            ("Project", "left"),
            ("Samples", "right"),
            ("ΔMsg", "right"),
            ("Msg/s", "right"),
            ("ΔOK", "right"),
            ("ΔFail", "right"),
            ("ΔHdl", "right"),
            ("Trend", "left"),
        ):
            table.add_column(heading, justify=justify)

        for run in status["runs"]:
            for history in run["history"]:
                deltas = history["deltas"]
                table.add_row(
                    run["command"],
                    history["project"],
                    str(history["sample_count"]),
                    str(deltas["consumed"]),
                    f"{history['message_rate']:.2f}",
                    str(deltas["succeeded"]),
                    str(deltas["failed"]),
                    str(deltas["handles"]),
                    _sparkline(history["message_rate_series"]),
                )
        if not any(run["history"] for run in status["runs"]):
            table.add_row("—", "no samples in window", *("—" for _ in range(7)))
        return table


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
