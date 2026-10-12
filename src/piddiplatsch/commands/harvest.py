"""Harvest command implementation."""

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import click

from piddiplatsch.commands.base import KafkaCommand
from piddiplatsch.config import config
from piddiplatsch.consumer import HarvestProcessor
from piddiplatsch.exceptions import CatalogError


@dataclass(kw_only=True)
class HarvestCommand(KafkaCommand):
    """Harvest raw Kafka dumps and project catalogs without Handle mapping."""

    idle_timeout: float = 5.0
    limit: int | None = None
    catalog: bool | None = None

    def execute(self) -> None:
        catalog_cfg = config.get("catalog", {})
        enabled = (
            catalog_cfg.get("enabled", True) if self.catalog is None else self.catalog
        )
        directory = None
        if enabled:
            directory = Path(
                catalog_cfg.get("directory")
                or Path(config.get("consumer", "output_dir", "outputs")) / "catalog"
            )
        try:
            with closing(
                HarvestProcessor(
                    catalog_dir=directory, backend=catalog_cfg.get("backend", "sqlite")
                )
            ) as processor:
                self.run_consumer(
                    title="harvest",
                    processor=processor,
                    force=True,
                    idle_timeout=self.idle_timeout,
                    limit=self.limit,
                )
        except (CatalogError, ValueError) as exc:
            raise click.ClickException(str(exc)) from exc
