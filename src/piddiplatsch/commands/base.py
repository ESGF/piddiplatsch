"""Base API shared by application commands."""

from abc import ABC, abstractmethod
from dataclasses import dataclass

from piddiplatsch.monitoring.progress import BoundedProgress


@dataclass(kw_only=True)
class Command(ABC):
    """A single application action exposed by the CLI.

    Command inputs are declared as dataclass fields by concrete commands. The
    caller constructs a command and invokes it through the uniform ``execute``
    API.
    """

    verbose: bool = False

    def progress(self, *, title: str, unit: str, start: int = 0) -> BoundedProgress:
        """Create bounded progress using this command's verbosity setting."""
        return BoundedProgress(title=title, unit=unit, enabled=self.verbose, start=start)

    @abstractmethod
    def execute(self) -> None:
        """Execute the command."""
