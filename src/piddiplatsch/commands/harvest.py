"""Harvest command implementation."""

from dataclasses import dataclass

from piddiplatsch.commands.base import KafkaCommand
from piddiplatsch.consumer import HarvestProcessor


@dataclass(kw_only=True)
class HarvestCommand(KafkaCommand):
    """Harvest Kafka messages without mapping them."""

    def execute(self) -> None:
        self.run_consumer(
            title="harvest",
            processor=HarvestProcessor(),
            force=True,
        )
