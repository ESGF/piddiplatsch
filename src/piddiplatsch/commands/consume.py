"""Consume command implementation."""

from dataclasses import dataclass

from piddiplatsch.commands.base import Command
from piddiplatsch.commands.helper import select_projects
from piddiplatsch.config import config
from piddiplatsch.consumer import start_consumer


@dataclass(kw_only=True)
class ConsumeCommand(Command):
    """Harvest and map Kafka messages."""

    publish: bool = False
    force: bool = False
    projects: tuple[str, ...] = ()
    all_projects: bool = False

    def execute(self) -> None:
        start_consumer(
            config.get("consumer", "topic"),
            config.get("kafka"),
            projects=select_projects(self.projects, self.all_projects),
            dump_messages=True,
            verbose=self.verbose,
            dry_run=not self.publish,
            force=self.force,
        )
