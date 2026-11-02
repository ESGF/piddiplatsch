"""Publish command implementation."""

from dataclasses import dataclass
from pathlib import Path

import click

from piddiplatsch.commands.base import FileBatchCommand
from piddiplatsch.result import PublishResult
from piddiplatsch.runners.publish import HandlePublisher


@dataclass(kw_only=True)
class PublishCommand(FileBatchCommand):
    """Publish prepared handles."""

    retries: int = 0
    retry_delay: float = 1.0
    workers: int = 4
    project: str | None = None
    handle_profile: str | None = None

    def execute(self) -> None:
        if not self.paths:
            self._publish_batches()
            return
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
                    handle_profile=self.handle_profile,
                )
            except (ValueError, OSError) as exc:
                raise click.ClickException(str(exc)) from exc

        self._show_result(result, progress.position)

    def _publish_batches(self):
        from piddiplatsch.exceptions import BatchError, CatalogError
        from piddiplatsch.runners.sync import SyncWorker

        self.validate_input()
        if (
            self.input_date not in (None, "all")
            or self.from_date
            or self.to_date
            or self.limit
            or self.offset
        ):
            raise click.UsageError(
                "Queued publication processes complete batches in order; date/limit/offset selectors require explicit files"
            )
        try:
            worker = SyncWorker(
                projects=(self.project,) if self.project else (),
                workers=self.workers,
                retries=self.retries,
                handle_profile=self.handle_profile,
                publisher=HandlePublisher(),
            )
            worker.run(publish_only=True)
            if worker.errors:
                raise BatchError("; ".join(worker.errors))
        except (BatchError, CatalogError, ValueError, OSError) as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(f"Published {worker.published_handles} Handles from queued batches.")

    def _resolve_paths(self) -> tuple[Path, ...]:
        from piddiplatsch.config import config

        self.validate_input()
        root = Path(
            config.get("batch", "directory")
            or Path(config.get("consumer", "output_dir", "outputs")) / "batches"
        ).resolve()
        if root.exists():
            for source in self.paths:
                path = source.resolve()
                if (
                    path == root
                    or root in path.parents
                    or (path.is_dir() and path in root.parents)
                ):
                    raise click.UsageError(
                        "Publish managed batches without PATH using --project, so saved progress and ordering are preserved"
                    )
        return self.paths

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
            if result.failed > len(result.errors):
                click.echo(
                    "  Further errors omitted; see publication results and logs."
                )
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
