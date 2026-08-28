"""Harvest command implementation."""

from dataclasses import dataclass

from piddiplatsch.commands.base import Command
from piddiplatsch.config import config
from piddiplatsch.consumer import HarvestProcessor, start_consumer


@dataclass(kw_only=True)
class HarvestCommand(Command):
    """Harvest Kafka messages without mapping them."""

    def execute(self) -> None:
        progress = self.progress(title="harvest", stream=True)
        with progress:
            start_consumer(
                config.get("consumer", "topic"),
                config.get("kafka"),
                processor=HarvestProcessor(),
                dump_messages=True,
                verbose=self.verbose,
                progress=progress,
                force=True,
            )
