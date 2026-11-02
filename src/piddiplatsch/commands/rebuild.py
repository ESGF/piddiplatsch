"""Operator-controlled work history consolidation."""

import sqlite3
from dataclasses import dataclass

import click

from piddiplatsch.commands.base import Command
from piddiplatsch.config import config
from piddiplatsch.exceptions import CatalogError
from piddiplatsch.runners.rebuild import rebuild_work
from piddiplatsch.runners.reset import processing_paths


@dataclass(kw_only=True)
class RebuildWorkCommand(Command):
    apply: bool = False
    vacuum: bool = False

    def execute(self):
        output, database, _, _ = processing_paths(config, None)
        click.echo(f"Keep catalog and Kafka checkpoints: {database}")
        click.echo("Keep processing summaries and published Handle totals.")
        click.echo(
            f"Archive old work, summaries and batches: {output / 'work-rebuild-archives'}"
        )
        click.echo(
            "Replace work history with one published full snapshot per catalog item, ordered by local last_update."
        )
        click.echo(
            "Requires all retained work to be published: no pending, mapped or invalid events. Nothing is requeued."
        )
        click.echo(
            "VACUUM: reclaim unused file space."
            if self.vacuum
            else "No VACUUM: leave unused space available for SQLite reuse."
        )
        if not self.apply:
            click.echo(
                "Preview only. Stop harvest, sync, publication and maintenance, then add --apply."
            )
            return
        try:
            result = rebuild_work(config, vacuum=self.vacuum)
        except (
            CatalogError,
            ValueError,
            OSError,
            RuntimeError,
            sqlite3.Error,
            KeyError,
            TypeError,
        ) as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(
            f"Rebuilt {result['previous_work_entries']:,} old work entries as "
            f"{result['catalog_entries']:,} published catalog snapshots. Archive: {result['archive']}"
        )
