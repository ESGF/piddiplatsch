"""Map command implementation."""

from dataclasses import dataclass
from pathlib import Path

import click

from piddiplatsch.commands.base import DatedBatchCommand
from piddiplatsch.commands.helper import select_projects
from piddiplatsch.config import config
from piddiplatsch.core.pipeline import configured_projects
from piddiplatsch.core.registry import get_plugins
from piddiplatsch.exceptions import CatalogError, MaxErrorsExceededError
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
            self._start_monitoring(selection)
            with progress:
                result = map_catalog(
                    directory=self.directory,
                    input_date=(
                        "last"
                        if self.input_date is None
                        and self.from_date is None
                        and self.to_date is None
                        else self.input_date
                    ),
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
        except (CatalogError, MaxErrorsExceededError, OSError, ValueError) as exc:
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
            click.echo("No catalog items found.")
            return
        click.echo(f"Mapped {result.succeeded}/{result.total} catalog items.")
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

    @staticmethod
    def _start_monitoring(selection: str | tuple[str, ...] | None) -> None:
        selected = configured_projects() if selection is None else selection
        project_names = tuple(plugin.name for plugin in get_plugins(selected))
        stats_config = config.get("stats", {})
        stats.configure_for_run(
            enable_db=stats_config.get("enable_db", False),
            db_path=stats_config.get("db_path"),
            log_interval_seconds=stats_config.get("interval_seconds"),
            log_interval_messages=stats_config.get("summary_interval"),
            command="map",
            selected_projects=project_names,
            heartbeat_interval_seconds=stats_config.get(
                "heartbeat_interval_seconds", 5
            ),
            sample_interval_seconds=stats_config.get("sample_interval_seconds", 15),
            sample_retention_days=stats_config.get("sample_retention_days", 30),
        )
