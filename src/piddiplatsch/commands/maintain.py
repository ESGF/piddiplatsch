import sqlite3
from dataclasses import dataclass

import click

from piddiplatsch.commands.base import Command
from piddiplatsch.config import config
from piddiplatsch.config.schema import MaintenanceConfig
from piddiplatsch.dump_maintenance import deduplicate_closed_dumps
from piddiplatsch.maintenance import (
    clean_published_work,
    clean_status_history,
    compress_closed_dumps,
)
from piddiplatsch.monitoring.disk import check_disk_space
from piddiplatsch.output_maintenance import maintain_outputs


@dataclass(kw_only=True)
class MaintainCommand(Command):
    dry_run: bool = False
    deduplicate_dumps: bool | None = None

    def execute(self):
        try:
            settings = MaintenanceConfig.model_validate(
                config.get("maintenance", {}) or {}
            )
            deduplicate = (
                settings.deduplicate_dumps
                if self.deduplicate_dumps is None
                else self.deduplicate_dumps
            )
            if not self.dry_run:
                check_disk_space(force=True)
            duplicates = (
                deduplicate_closed_dumps(dry_run=self.dry_run, progress=click.echo)
                if deduplicate
                else 0
            )
            paths = compress_closed_dumps(dry_run=self.dry_run)
            outputs = maintain_outputs(dry_run=self.dry_run)
            removed = clean_published_work(dry_run=self.dry_run)
            clean_status_history(dry_run=self.dry_run)
        except (OSError, ValueError, sqlite3.Error) as exc:
            raise click.ClickException(str(exc)) from exc
        if self.dry_run:
            for path in paths:
                click.echo(str(path))
        action = "Would compress" if self.dry_run else "Compressed"
        if deduplicate:
            click.echo(
                f"{'Would remove' if self.dry_run else 'Removed'} {duplicates} duplicate dump event(s)."
            )
        click.echo(f"{action} {len(paths)} closed dump file(s).")
        if self.dry_run:
            for path in outputs.compressed:
                click.echo(f"Would compress output: {path}")
            for path in (*outputs.removed_batches, *outputs.removed_receipts):
                click.echo(f"Would delete output: {path}")
        click.echo(f"{action} {len(outputs.compressed)} completed output file(s).")
        if config.get("maintenance", "output_retention_days", 0) == 0:
            click.echo(
                "Output removal disabled (maintenance.output_retention_days = 0)."
            )
        else:
            action = "Would delete" if self.dry_run else "Deleted"
            click.echo(
                f"{action} {len(outputs.removed_batches)} completed batch(es) and {len(outputs.removed_receipts)} receipt file(s)."
            )
        if config.get("batch", "retention_days", 0) == 0:
            click.echo("Database work cleanup disabled (batch.retention_days = 0).")
        else:
            action = "Would delete" if self.dry_run else "Deleted"
            click.echo(f"{action} {removed} expired published work entries.")
