"""Harvest command implementation."""

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

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
    archives: tuple[Path, ...] = ()
    ignore_unknown_projects: bool | None = None

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
        if self.ignore_unknown_projects is not None and not enabled:
            raise click.UsageError("Unknown-project flags require --catalog")
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
                    ignore_unknown_projects=self.ignore_unknown_projects,
                )
            ) as processor:
                if clean_projects:
                    click.echo(
                        f"Cleared catalogs: {', '.join(clean_projects)}. Replaying retained Kafka history."
                    )
                if self.archives:
                    if not enabled:
                        raise ValueError("Archive replay requires the database catalog")
                    from piddiplatsch.catalog.replay import archived_messages
                    from piddiplatsch.consumer import start_consumer

                    with (
                        closing(archived_messages(self.archives)) as messages,
                        self.progress(title="harvest replay", stream=True) as progress,
                    ):
                        start_consumer(
                            processor=processor,
                            direct_messages=messages,
                            limit=self.limit,
                            monitor_db=True,
                            progress=progress,
                        )
                    return
                self.run_consumer(
                    title="harvest",
                    processor=processor,
                    idle_timeout=None if self.idle_timeout == 0 else self.idle_timeout,
                    limit=self.limit,
                    monitor_db=True,
                )
        except (CatalogError, ValueError, OSError) as exc:
            raise click.ClickException(str(exc)) from exc
