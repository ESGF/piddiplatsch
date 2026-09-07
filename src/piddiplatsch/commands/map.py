"""Map command implementation."""

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import click

from piddiplatsch.commands.base import FileBatchCommand
from piddiplatsch.commands.helper import select_projects
from piddiplatsch.config import config
from piddiplatsch.consumer import map_dump_files
from piddiplatsch.exceptions import JsonlReadError


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
            raise click.ClickException(str(exc)) from exc

        if result.total == 0:
            click.echo("No dumped messages found.")
            return
        click.echo(f"Mapped {result.succeeded}/{result.total} dumped messages.")
        if result.filtered:
            click.echo(f"Filtered by project selection: {result.filtered}")
        if result.skipped:
            click.echo(f"Skipped: {result.skipped}")
        if result.failed:
            click.echo(f"Failed: {result.failed}")
            raise click.exceptions.Exit(1)

    def _resolve_paths(self) -> tuple[Path, ...]:
        if self.input_date == "last":
            self.validate_input()
            return (self._latest_dump(),)
        return self.resolve_paths(
            relative_path=lambda date: Path("dump") / f"dump_messages_{date}.jsonl",
            missing_label="Raw dump",
        )

    @staticmethod
    def _latest_dump() -> Path:
        output_dir = Path(config.get("consumer", {}).get("output_dir", "outputs"))
        dump_dir = output_dir / "dump"
        candidates: list[tuple[str, Path]] = []
        prefix = "dump_messages_"
        suffix = ".jsonl"
        for path in dump_dir.glob(f"{prefix}*{suffix}"):
            date_text = path.name[len(prefix) : -len(suffix)]
            try:
                parsed_date = date.fromisoformat(date_text)
            except ValueError:
                continue
            if path.is_file() and parsed_date.isoformat() == date_text:
                candidates.append((date_text, path))

        if not candidates:
            raise click.ClickException(f"No dated raw dumps found in: {dump_dir}")
        return max(candidates)[1]
