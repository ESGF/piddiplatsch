"""Historical monitoring command."""

import json
from dataclasses import dataclass
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich.text import Text

from piddiplatsch.monitoring.insights import METRICS, read_insights

from .base import Command


@dataclass(kw_only=True)
class InsightsCommand(Command):
    db_path: Path
    days: int = 30
    project: str | None = None
    json_output: bool = False
    summary: bool = False

    def execute(self) -> None:
        try:
            history = read_insights(self.db_path, days=self.days, project=self.project)
        except (FileNotFoundError, ValueError) as exc:
            raise click.ClickException(str(exc)) from exc
        if self.json_output:
            click.echo(json.dumps(history, indent=2))
            return
        console = Console()
        console.print(
            f"Processing history: {history['from_date']} to {history['to_date']} (UTC)"
        )
        for warning in history["warnings"]:
            console.print(Text(warning, style="yellow"))
        rows = history["totals"] if self.summary else history["daily"]
        if not rows:
            console.print("No recorded activity in this period.")
            return
        table = Table(box=None)
        if not self.summary:
            table.add_column("Day")
        table.add_column("Project")
        for label in (
            "Events",
            "Mapped",
            "Prepared handles",
            "Published handles",
            "Errors",
        ):
            table.add_column(label, justify="right")
        for row in rows:
            table.add_row(
                *([] if self.summary else [row["day"]]),
                Text(row["project"]),
                *(f"{row[metric]:,}" for metric in METRICS),
            )
        console.print(table)
        console.print(
            "Recorded monitoring activity; missing days may have no activity or no retained data.",
            style="dim",
        )
