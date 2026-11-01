"""Reset one project's processing while preserving its harvested catalog."""

import sqlite3
from dataclasses import dataclass

import click

from piddiplatsch.commands.base import Command
from piddiplatsch.config import config
from piddiplatsch.core.registry import get_plugin
from piddiplatsch.exceptions import CatalogError
from piddiplatsch.runners.reset import processing_paths, reset_processing


@dataclass(kw_only=True)
class ResetCommand(Command):
    project: str
    apply: bool = False

    def execute(self) -> None:
        try:
            project = get_plugin(self.project).name
            output, catalog, batches, statistics = processing_paths(config, project)
            click.echo(f"Keep STAC records and Kafka checkpoints: {catalog}")
            click.echo(
                f"Archive old output: {batches / project} and {output / project}"
            )
            click.echo(f"Clear {project} monitoring counters: {statistics}")
            if not self.apply:
                click.echo(
                    "Preview only. Stop harvest and sync, then add --apply to perform the reset."
                )
                return
            count, backup = reset_processing(config, project)
        except (CatalogError, ValueError, OSError, RuntimeError, sqlite3.Error) as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(f"Queued {count:,} existing {project} items. Backups: {backup}")
