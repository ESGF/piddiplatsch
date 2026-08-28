"""Harvest command implementation."""

from dataclasses import dataclass

from piddiplatsch.commands.base import KafkaCommand
from piddiplatsch.consumer import HarvestProcessor


@dataclass(kw_only=True)
class HarvestCommand(KafkaCommand):
    """Harvest Kafka messages without mapping them."""

    idle_timeout: float = 5.0

    def execute(self) -> None:
        self.run_consumer(
            title="harvest",
            processor=HarvestProcessor(),
            force=True,
            idle_timeout=self.idle_timeout,
        )
