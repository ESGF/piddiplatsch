"""Base API shared by application commands."""

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, overload

import click

from piddiplatsch.commands.helper import resolve_dated_input
from piddiplatsch.config import config
from piddiplatsch.consumer import start_consumer
from piddiplatsch.monitoring.progress import (
    STREAM_PROGRESS_LEGEND,
    BaseProgress,
    BoundedProgress,
    get_progress,
)


@dataclass(kw_only=True)
class Command(ABC):
    """A single application action exposed by the CLI.

    Command inputs are declared as dataclass fields by concrete commands. The
    caller constructs a command and invokes it through the uniform ``execute``
    API.
    """

    verbose: bool = False

    @overload
    def progress(
        self,
        *,
        title: str,
        stream: Literal[False] = False,
        unit: str = "item",
        start: int = 0,
    ) -> BoundedProgress: ...

    @overload
    def progress(
        self,
        *,
        title: str,
        stream: Literal[True],
        unit: str = "item",
        start: int = 0,
    ) -> BaseProgress: ...

    def progress(
        self,
        *,
        title: str,
        stream: bool = False,
        unit: str = "item",
        start: int = 0,
    ) -> BaseProgress:
        """Create the requested progress style using this command's verbosity."""
        if stream and self.verbose:
            click.echo(STREAM_PROGRESS_LEGEND)
        return get_progress(
            title=title,
            use_tqdm=self.verbose,
            stream=stream,
            unit=unit,
            start=start,
        )

    @abstractmethod
    def execute(self) -> None:
        """Execute the command."""


@dataclass(kw_only=True)
class KafkaCommand(Command, ABC):
    """Common implementation for commands backed by the Kafka consumer."""

    def run_consumer(
        self,
        *,
        title: str,
        processor: object | None = None,
        projects: list[str] | tuple[str, ...] | str | None = None,
        dry_run: bool = False,
        force: bool = False,
        idle_timeout: float | None = None,
    ) -> None:
        progress = self.progress(title=title, stream=True)
        with progress:
            start_consumer(
                config.get("consumer", "topic"),
                config.get("kafka"),
                processor=processor,
                projects=projects,
                dump_messages=True,
                verbose=self.verbose,
                progress=progress,
                dry_run=dry_run,
                force=force,
                idle_timeout=idle_timeout,
            )


@dataclass(kw_only=True)
class FileBatchCommand(Command, ABC):
    """Common inputs and path resolution for dated file batches."""

    paths: tuple[Path, ...] = ()
    input_date: datetime | None = None
    limit: int | None = None
    offset: int = 0

    def validate_input(self) -> None:
        """Validate the mutually exclusive explicit-path and date inputs."""
        if self.paths and self.input_date is not None:
            raise click.UsageError("PATH cannot be combined with --date")

    def resolve_paths(
        self,
        *,
        relative_path: Callable[[str], Path],
        missing_label: str,
    ) -> tuple[Path, ...]:
        """Resolve explicit paths or a date-derived path below the output directory."""
        self.validate_input()
        date = self.input_date.date().isoformat() if self.input_date is not None else ""
        return resolve_dated_input(
            self.paths,
            self.input_date,
            relative_path=relative_path(date),
            missing_label=missing_label,
        )
