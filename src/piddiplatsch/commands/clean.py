"""Manual outbox cleanup."""

import sqlite3
from dataclasses import dataclass

import click

from piddiplatsch.commands.base import Command
from piddiplatsch.config import config
from piddiplatsch.exceptions import CatalogError
from piddiplatsch.runners.clean import clean_outbox


@dataclass(kw_only=True)
class CleanCommand(Command):
    apply: bool = False
    vacuum: bool = False

    def execute(self):
        click.echo(
            "Clear completed outbox records; keep work history, catalog, statistics and Kafka checkpoints."
        )
        click.echo(
            "Keep unfinished batches and completion records with exports still in ready folders. Keep the empty tables for future sync."
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
            counts = clean_outbox(config, vacuum=self.vacuum)
        except (CatalogError, ValueError, OSError, RuntimeError, sqlite3.Error) as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(
            f"Cleared outbox: {counts['outbox_batches']:,} batch records, {counts['handle_outbox']:,} Handle rows."
        )
