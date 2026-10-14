"""Scheduled mapping and publication command."""

from dataclasses import dataclass

import click

from piddiplatsch.commands.base import Command
from piddiplatsch.exceptions import BatchError, CatalogError
from piddiplatsch.runners.batch import BatchWorker


@dataclass(kw_only=True)
class BatchCommand(Command):
    force_map: bool = False

    def execute(self) -> None:
        progress = self.progress(title="batch", stream=True)
        try:
            worker = BatchWorker(progress=progress)
            with progress:
                batches = worker.run(force_map=self.force_map)
        except (BatchError, CatalogError, OSError, ValueError, RuntimeError) as exc:
            raise click.ClickException(str(exc)) from exc
        for batch in batches:
            click.echo(f"Sealed batch: {batch}")
        for error in worker.errors:
            click.echo(error, err=True)
        if worker.errors:
            raise click.exceptions.Exit(1)
        click.echo(f"Batch tick complete; sealed {len(batches)} batch(es).")
