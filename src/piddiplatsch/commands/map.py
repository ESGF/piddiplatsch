"""Map command implementation."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import click

from piddiplatsch.commands.base import Command
from piddiplatsch.commands.helper import resolve_dated_input, select_projects
from piddiplatsch.consumer import map_dump_files
from piddiplatsch.exceptions import JsonlReadError


@dataclass(kw_only=True)
class MapCommand(Command):
    """Map dumped messages through the selected project plugins."""

    paths: tuple[Path, ...] = ()
    input_date: datetime | None = None
    projects: tuple[str, ...] = ()
    all_projects: bool = False
    limit: int | None = None
    offset: int = 0
    force: bool = False

    def execute(self) -> None:
        progress = self.progress(title="map", stream=True)
        selection = select_projects(self.projects, self.all_projects)
        date = self.input_date.date().isoformat() if self.input_date is not None else ""
        paths = resolve_dated_input(
            self.paths,
            self.input_date,
            relative_path=Path("dump") / f"dump_messages_{date}.jsonl",
            missing_label="Raw dump",
        )
        try:
            with progress:
                result = map_dump_files(
                    paths,
                    projects=selection,
                    limit=self.limit,
                    offset=self.offset,
                    force=self.force,
                    verbose=self.verbose,
                    progress=progress,
                )
        except (JsonlReadError, OSError, ValueError) as exc:
            raise click.ClickException(str(exc)) from exc

        if result.total == 0:
            click.echo("No dumped messages found.")
            return
        click.echo(f"Mapped {result.succeeded}/{result.total} dumped messages.")
        if result.filtered:
            click.echo(f"Filtered by project selection: {result.filtered}")
        if result.skipped:
            click.echo(f"Skipped: {result.skipped}")
        if result.failed:
            click.echo(f"Failed: {result.failed}")
            raise click.exceptions.Exit(1)
