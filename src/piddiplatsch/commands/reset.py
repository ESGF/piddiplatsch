"""Reset one project's processing while preserving its harvested catalog."""

import sqlite3
from dataclasses import dataclass
from datetime import datetime

import click

from piddiplatsch.commands.base import Command
from piddiplatsch.config import config
from piddiplatsch.core.registry import get_plugin
from piddiplatsch.exceptions import CatalogError
from piddiplatsch.runners.reset import preview_reset, processing_paths, reset_processing


@dataclass(kw_only=True)
class ResetCommand(Command):
    project: str
    apply: bool = False
    from_date: datetime | None = None

    def execute(self) -> None:
        try:
            project = get_plugin(self.project).name
            output, catalog, batches, statistics = processing_paths(config, project)
            click.echo(f"Keep STAC records and Kafka checkpoints: {catalog}")
            selection = preview_reset(config, project, from_date=self.from_date)
            if self.from_date is None:
                click.echo(
                    f"Archive old output: {batches / project} and {output / project}"
                )
                click.echo(f"Clear {project} daily summaries: {statistics}")
            else:
                click.echo(
                    f"Requested creation date: {self.from_date:%Y-%m-%d} UTC onward."
                )
                click.echo(
                    "Archive affected batches/receipts and clear their summaries; preserve earlier completed history."
                )
            if selection is not None:
                click.echo(f"Selected retained work: {selection.count:,} entries.")
                if self.from_date is not None and selection.count:
                    click.echo(
                        f"Effective boundary: work ID {selection.first_id}, {selection.effective_from}."
                    )
                    click.echo(
                        f"Complete batches included: {len(selection.batch_ids):,}."
                    )
                    if selection.effective_from < selection.requested_from:
                        click.echo(
                            "Selection expanded before the requested date to preserve complete batches and work-ID order."
                        )
            if not self.apply:
                click.echo(
                    "Preview only. Stop harvest and sync, then add --apply to perform the reset."
                )
                return
            count, archive = reset_processing(config, project, from_date=self.from_date)
        except (CatalogError, ValueError, OSError, RuntimeError, sqlite3.Error) as exc:
            raise click.ClickException(str(exc)) from exc
        if archive is None:
            click.echo("No retained work matches the requested date; nothing changed.")
            return
        click.echo(
            f"Queued {count:,} retained {project} work entries. Output archive: {archive}"
        )
