"""Base API shared by application commands."""

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, overload

import click

from piddiplatsch.commands.helper import resolve_dated_input, validate_date_selection
from piddiplatsch.config import config
from piddiplatsch.monitoring.progress import (
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
        mode: str | None = None,
    ) -> BoundedProgress: ...

    @overload
    def progress(
        self,
        *,
        title: str,
        stream: Literal[True],
        unit: str = "item",
        start: int = 0,
        mode: str | None = None,
    ) -> BaseProgress: ...

    def progress(
        self,
        *,
        title: str,
        stream: bool = False,
        unit: str = "item",
        start: int = 0,
        mode: str | None = None,
    ) -> BaseProgress:
        """Create the requested progress style using this command's verbosity."""
        return get_progress(
            title=title,
            use_tqdm=self.verbose,
            stream=stream,
            unit=unit,
            start=start,
            mode=mode,
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
        publish: bool = False,
        force: bool = False,
        idle_timeout: float | None = None,
        limit: int | None = None,
        monitor_db: bool = False,
        handle_profile: str | None = None,
    ) -> None:
        from piddiplatsch.consumer import start_consumer

        mode = None
        if title == "harvest":
            mode = (
                "harvest"
                if getattr(processor, "requires_kafka_source", False)
                else "harvest_raw"
            )
        progress = self.progress(title=title, stream=True, mode=mode)
        with progress:
            start_consumer(
                config.get("consumer", "topic"),
                config.get("kafka"),
                processor=processor,
                projects=projects,
                dump_messages=True,
                verbose=self.verbose,
                progress=progress,
                publish=publish,
                force=force,
                idle_timeout=idle_timeout,
                limit=limit,
                monitor_db=monitor_db,
                handle_profile=handle_profile,
            )


@dataclass(kw_only=True)
class DatedBatchCommand(Command, ABC):
    """Common day/range and position selections for map and publish."""

    input_date: datetime | str | None = None
    from_date: datetime | None = None
    to_date: datetime | None = None
    limit: int | None = None
    offset: int = 0

    def validate_input(self) -> None:
        try:
            validate_date_selection(
                self.input_date,
                self.from_date,
                self.to_date,
                has_paths=bool(getattr(self, "paths", ())),
            )
        except ValueError as exc:
            raise click.UsageError(str(exc)) from exc


@dataclass(kw_only=True)
class FileBatchCommand(DatedBatchCommand, ABC):
    """Common path resolution for dated file batches."""

    paths: tuple[Path, ...] = ()

    def resolve_paths(
        self,
        *,
        relative_path: Callable[[str], Path],
        missing_label: str,
    ) -> tuple[Path, ...]:
        """Resolve explicit paths or a date-derived path below the output directory."""
        self.validate_input()
        if isinstance(self.input_date, str):
            raise ValueError(f"Unresolved date selector: {self.input_date}")
        date = self.input_date.date().isoformat() if self.input_date is not None else ""
        return resolve_dated_input(
            self.paths,
            self.input_date,
            relative_path=relative_path(date),
            missing_label=missing_label,
        )
