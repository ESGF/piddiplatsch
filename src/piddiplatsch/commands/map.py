"""Map command implementation."""

from dataclasses import dataclass
from pathlib import Path

import click

from piddiplatsch.commands.base import FileBatchCommand
from piddiplatsch.commands.helper import resolve_latest_dated_input, select_projects
from piddiplatsch.config import config
from piddiplatsch.consumer import configured_projects, map_dump_files
from piddiplatsch.core.registry import get_plugins
from piddiplatsch.exceptions import JsonlReadError
from piddiplatsch.monitoring.stats import stats


@dataclass(kw_only=True)
class MapCommand(FileBatchCommand):
    """Map dumped messages through the selected project plugins."""

    projects: tuple[str, ...] = ()
    all_projects: bool = False
    force: bool = False
    handle_profile: str | None = None

    def execute(self) -> None:
        progress = self.progress(title="map", stream=True)
        selection = select_projects(self.projects, self.all_projects)
        paths = self._resolve_paths()
        try:
            self._start_monitoring(selection)
            with progress:
                result = map_dump_files(
                    paths,
                    projects=selection,
                    limit=self.limit,
                    offset=self.offset,
                    force=self.force,
                    verbose=self.verbose,
                    progress=progress,
                    handle_profile=self.handle_profile,
                )
        except (JsonlReadError, OSError, ValueError) as exc:
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
            click.echo("No dumped messages found.")
            return
        click.echo(f"Mapped {result.succeeded}/{result.total} dumped messages.")
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

    def _resolve_paths(self) -> tuple[Path, ...]:
        if not self.paths and self.input_date in (None, "last"):
            self.validate_input()
            return resolve_latest_dated_input(
                relative_dir=Path("dump"),
                filename_prefix="dump_messages_",
                missing_label="raw dump",
            )
        return self.resolve_paths(
            relative_path=lambda date: Path("dump") / f"dump_messages_{date}.jsonl",
            missing_label="Raw dump",
        )
