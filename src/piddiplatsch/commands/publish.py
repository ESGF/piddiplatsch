"""Publish command implementation."""

from dataclasses import dataclass
from pathlib import Path

import click

from piddiplatsch.commands.base import FileBatchCommand
from piddiplatsch.core.plugin import normalize_project_id
from piddiplatsch.handles.publish import HandlePublisher
from piddiplatsch.result import PublishResult


@dataclass(kw_only=True)
class PublishCommand(FileBatchCommand):
    """Publish prepared handles."""

    retries: int = 0
    retry_delay: float = 1.0
    workers: int = 1
    project: str | None = None

    def execute(self) -> None:
        paths = self._resolve_paths()
        progress_label = (
            f"publish {self.project} handles" if self.project else "publish handles"
        )
        progress = self.progress(title=progress_label, unit="handle", start=self.offset)

        def show_progress(index, total, handle, error):
            progress.update(
                total=total,
                position=self.offset + index,
                ok=error is None,
            )

        with progress:
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

        self._show_result(result, progress.position)

    def _resolve_paths(self) -> tuple[Path, ...]:
        self.validate_input()
        project_name = normalize_project_id(self.project or "")
        if self.input_date is not None and not project_name:
            raise click.UsageError("--date requires --project")
        return self.resolve_paths(
            relative_path=lambda date: Path(project_name)
            / "handles"
            / f"handles_{date}.jsonl",
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
            click.echo(
                f"  {project_name}: {project_result.succeeded}/{project_result.total} published, {project_result.failed} failed"
            )
