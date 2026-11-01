from dataclasses import dataclass

import click

from piddiplatsch.commands.base import Command
from piddiplatsch.config import config
from piddiplatsch.maintenance import (
    clean_published_work,
    clean_status_history,
    compress_closed_dumps,
)


@dataclass(kw_only=True)
class MaintainCommand(Command):
    dry_run: bool = False

    def execute(self):
        try:
            paths = compress_closed_dumps(dry_run=self.dry_run)
            removed = clean_published_work(dry_run=self.dry_run)
            clean_status_history(dry_run=self.dry_run)
        except (OSError, ValueError) as exc:
            raise click.ClickException(str(exc)) from exc
        if self.dry_run:
            for path in paths:
                click.echo(str(path))
        action = "Would compress" if self.dry_run else "Compressed"
        click.echo(f"{action} {len(paths)} closed dump file(s).")
        if config.get("batch", "retention_days", 0) == 0:
            click.echo("Database work cleanup disabled (batch.retention_days = 0).")
        else:
            action = "Would delete" if self.dry_run else "Deleted"
            click.echo(f"{action} {removed} expired published work entries.")
