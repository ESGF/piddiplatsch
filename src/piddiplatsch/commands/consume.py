"""Consume command implementation."""

from dataclasses import dataclass

from piddiplatsch.commands.base import KafkaCommand
from piddiplatsch.commands.helper import select_projects


@dataclass(kw_only=True)
class ConsumeCommand(KafkaCommand):
    """Harvest and map Kafka messages."""

    publish: bool = False
    force: bool = False
    projects: tuple[str, ...] = ()
    all_projects: bool = False
    handle_profile: str | None = None

    def execute(self) -> None:
        self.run_consumer(
            title="consume",
            projects=select_projects(self.projects, self.all_projects),
            dry_run=not self.publish,
            force=self.force,
            handle_profile=self.handle_profile,
        )
