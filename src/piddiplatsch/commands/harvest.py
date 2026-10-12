"""Harvest command implementation."""

from contextlib import closing
from dataclasses import dataclass

import click

from piddiplatsch.catalog.api import get_catalog_directory
from piddiplatsch.commands.base import KafkaCommand
from piddiplatsch.config import config
from piddiplatsch.consumer import HarvestProcessor
from piddiplatsch.core.registry import get_plugins
from piddiplatsch.exceptions import CatalogError


@dataclass(kw_only=True)
class HarvestCommand(KafkaCommand):
    """Harvest raw Kafka dumps and project catalogs without Handle mapping."""

    idle_timeout: float = 5.0
    limit: int | None = None
    catalog: bool | None = None
    clean_catalog: bool = False
    clean_projects: tuple[str, ...] = ()

    def execute(self) -> None:
        catalog_cfg = config.get("catalog", {})
        enabled = (
            catalog_cfg.get("enabled", True) if self.catalog is None else self.catalog
        )
        if self.clean_catalog and self.clean_projects:
            raise click.UsageError(
                "--clean-catalog cannot be combined with --clean-project"
            )
        if (self.clean_catalog or self.clean_projects) and not enabled:
            raise click.UsageError(
                "Catalog cleaning requires --catalog; it cannot run with catalogs disabled"
            )
        clean_projects = ()
        if self.clean_catalog or self.clean_projects:
            try:
                clean_projects = tuple(
                    plugin.name
                    for plugin in get_plugins(
                        "all" if self.clean_catalog else self.clean_projects
                    )
                )
            except ValueError as exc:
                raise click.UsageError(str(exc)) from exc
        directory = None
        if enabled:
            directory = get_catalog_directory()
        try:
            with closing(
                HarvestProcessor(
                    catalog_dir=directory,
                    backend=catalog_cfg.get("backend", "sqlite"),
                    clean_projects=clean_projects,
                )
            ) as processor:
                if clean_projects:
                    click.echo(
                        f"Cleared catalogs: {', '.join(clean_projects)}. Replaying retained Kafka history."
                    )
                self.run_consumer(
                    title="harvest",
                    processor=processor,
                    force=True,
                    idle_timeout=self.idle_timeout,
                    limit=self.limit,
                )
        except (CatalogError, ValueError) as exc:
            raise click.ClickException(str(exc)) from exc
