"""Base API shared by application commands."""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(kw_only=True)
class Command(ABC):
    """A single application action exposed by the CLI.

    Command inputs are declared as dataclass fields by concrete commands. The
    caller constructs a command and invokes it through the uniform ``execute``
    API.
    """

    verbose: bool = False

    @abstractmethod
    def execute(self) -> None:
        """Execute the command."""
