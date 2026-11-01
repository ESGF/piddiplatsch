"""Map command implementation."""

from dataclasses import dataclass
from pathlib import Path

import click

from piddiplatsch.commands.base import DatedBatchCommand
from piddiplatsch.commands.helper import select_projects
from piddiplatsch.exceptions import BatchError, CatalogError, MaxErrorsExceededError
from piddiplatsch.monitoring.stats import stats
from piddiplatsch.runners.mapping import map_catalog


@dataclass(kw_only=True)
class MapCommand(DatedBatchCommand):
    """Map complete cached STAC items through the selected project plugins."""

    projects: tuple[str, ...] = ()
    all_projects: bool = False
    directory: Path | None = None
    version_history: bool | None = None
    handle_profile: str | None = None

    def execute(self) -> None:
        self.validate_input()
        progress = self.progress(title="map", stream=True)
        selection = select_projects(self.projects, self.all_projects)
        try:
            with progress:
                result = map_catalog(
                    directory=self.directory,
                    input_date=self.input_date,
                    from_date=self.from_date,
                    to_date=self.to_date,
                    projects=selection,
                    limit=self.limit,
                    offset=self.offset,
                    version_history=self.version_history,
                    verbose=self.verbose,
                    progress=progress,
                    handle_profile=self.handle_profile,
                )
        except (
            BatchError,
            CatalogError,
            MaxErrorsExceededError,
            OSError,
            ValueError,
        ) as exc:
            stats.close(status="failed", error_summary=str(exc))
            raise click.ClickException(str(exc)) from exc
        except KeyboardInterrupt:
            stats.close(status="stopped")
            raise
        except Exception as exc:
            stats.close(status="failed", error_summary=str(exc))
            raise

        if result.total == 0:
            stats.close(status="completed")
            click.echo("No pending work found.")
            return
        click.echo(f"Mapped {result.succeeded}/{result.total} work entries.")
        if result.filtered:
            click.echo(f"Filtered by project selection: {result.filtered}")
        if result.skipped:
            click.echo(f"Skipped: {result.skipped}")
        if result.failed:
            click.echo(f"Failed: {result.failed}")
            stats.close(
                status="failed",
                error_summary=f"{result.failed} mapping messages failed",
            )
            raise click.exceptions.Exit(1)
        stats.close(status="completed")
