"""Base API shared by application commands."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, overload

from piddiplatsch.monitoring.progress import BaseProgress, BoundedProgress, get_progress


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
