"""Publish command implementation."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import click
from tqdm import tqdm

from piddiplatsch.commands.base import Command
from piddiplatsch.commands.helper import resolve_dated_input
from piddiplatsch.core.plugin import normalize_project_id
from piddiplatsch.handles.publish import HandlePublisher
from piddiplatsch.result import PublishResult


@dataclass(kw_only=True)
class PublishCommand(Command):
    """Publish prepared handles."""

    paths: tuple[Path, ...] = ()
    input_date: datetime | None = None
    limit: int | None = None
    offset: int = 0
    retries: int = 0
    retry_delay: float = 1.0
    workers: int = 1
    project: str | None = None

    def execute(self) -> None:
        paths = self._resolve_paths()
        last_handle_position = self.offset
        progress_bar = None
        progress_succeeded = 0
        progress_failed = 0

        def show_progress(index, total, handle, error):
            nonlocal last_handle_position, progress_bar, progress_succeeded, progress_failed
            last_handle_position = max(last_handle_position, self.offset + index)
            if not self.verbose:
                return
            if progress_bar is None:
                progress_label = f"publish {self.project} handles" if self.project else "publish handles"
                progress_bar = tqdm(
                    total=total,
                    desc=f"{progress_label} {self.offset + 1}-{self.offset + total}",
                    unit="handle",
                    dynamic_ncols=True,
                )
            if error is None:
                progress_succeeded += 1
            else:
                progress_failed += 1
            progress_bar.set_postfix(position=last_handle_position, ok=progress_succeeded, failed=progress_failed)
            progress_bar.update(1)

        try:
            try:
                result = HandlePublisher().run(
                    paths,
                    limit=self.limit,
                    offset=self.offset,
                    retries=self.retries,
                    retry_delay=self.retry_delay,
                    workers=self.workers,
                    progress_callback=show_progress,
                    project=self.project,
                )
            except ValueError as exc:
                raise click.ClickException(str(exc)) from exc
        finally:
            if progress_bar is not None:
                progress_bar.close()

        self._show_result(result, last_handle_position)

    def _resolve_paths(self) -> tuple[Path, ...]:
        if self.paths and self.input_date is not None:
            raise click.UsageError("PATH cannot be combined with --date")
        project_name = normalize_project_id(self.project or "")
        if self.input_date is not None and not project_name:
            raise click.UsageError("--date requires --project")
        date = self.input_date.date().isoformat() if self.input_date is not None else ""
        return resolve_dated_input(
            self.paths,
            self.input_date,
            relative_path=Path(project_name) / "handles" / f"handles_{date}.jsonl",
            missing_label="Handle file",
        )

    def _show_result(self, result: PublishResult, last_handle_position: int) -> None:
        if result.total == 0:
            click.echo("No handles found.")
            return

        click.echo(f"Published {result.succeeded}/{result.total} handles.")
        self._show_projects(result)
        if result.result_file is not None:
            click.echo(f"Publication results: {result.result_file}")
        if last_handle_position > self.offset:
            click.echo(f"Processed handles: {self.offset + 1}-{last_handle_position}.")
        if self.limit is not None and result.total == self.limit:
            click.echo(f"Stopped after reaching the limit of {self.limit} handles.")
        if result.retry_attempts:
            click.echo(f"Retry attempts: {result.retry_attempts}")
        if result.failed:
            click.echo(f"Failed: {result.failed}")
            for error in result.errors:
                click.echo(f"  - {error}")
            raise click.exceptions.Exit(1)

    @staticmethod
    def _show_projects(result: PublishResult) -> None:
        if not result.projects:
            return
        click.echo("Projects:")
        for project_name, project_result in sorted(result.projects.items()):
            click.echo(f"  {project_name}: {project_result.succeeded}/{project_result.total} published, {project_result.failed} failed")
