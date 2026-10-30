from dataclasses import dataclass

import click

from piddiplatsch.commands.base import Command
from piddiplatsch.maintenance import compress_closed_dumps


@dataclass(kw_only=True)
class MaintainCommand(Command):
    dry_run: bool = False

    def execute(self):
        try:
            paths = compress_closed_dumps(dry_run=self.dry_run)
        except (OSError, ValueError) as exc:
            raise click.ClickException(str(exc)) from exc
        if self.dry_run:
            for path in paths:
                click.echo(str(path))
        action = "Would compress" if self.dry_run else "Compressed"
        click.echo(f"{action} {len(paths)} closed dump file(s).")
