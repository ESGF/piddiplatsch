"""Stream complete local catalog items through the shared processing pipeline."""

import logging
from contextlib import ExitStack, closing
from datetime import date, datetime
from itertools import chain, islice
from pathlib import Path

from piddiplatsch.catalog.api import (
    get_catalog_directory,
    get_catalog_path,
    open_catalog,
)
from piddiplatsch.catalog.state import HarvestState
from piddiplatsch.commands.helper import validate_date_selection
from piddiplatsch.config import config
from piddiplatsch.core.pipeline import (
    build_processing_target,
    configured_projects,
    process_messages,
)
from piddiplatsch.core.registry import get_plugins
from piddiplatsch.exceptions import CatalogError
from piddiplatsch.monitoring.progress import BaseProgress
from piddiplatsch.result import FeedResult

logger = logging.getLogger(__name__)


def map_catalog(
    *,
    directory: Path | None = None,
    input_date: datetime | str | None = None,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    projects: list[str] | tuple[str, ...] | str | None = None,
    limit: int | None = None,
    offset: int = 0,
    verbose: bool = False,
    progress: BaseProgress | None = None,
    handle_profile: str | None = None,
    version_history: bool | None = None,
) -> FeedResult:
    """Map read-only per-project snapshots; no raw event replay or STAC fetches.

    Explicit selections require every selected catalog to exist. ``all`` selects
    available registered catalogs. Dated selections order by project, event day,
    then collection/item identity; unfiltered reads retain identity order.
    Offsets and limits count items after filtering across projects.
    """
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")
    if offset < 0:
        raise ValueError("offset cannot be negative")
    validate_date_selection(input_date, from_date, to_date)

    directory = Path(directory) if directory is not None else get_catalog_directory()
    if HarvestState(directory).pending_clean:
        raise CatalogError("Catalog clean is incomplete; run harvest before mapping")
    selection = configured_projects() if projects is None else projects
    plugins = get_plugins(selection)
    available = []
    for plugin in plugins:
        if get_catalog_path(plugin.name, directory=directory).is_file():
            available.append(plugin)
        elif selection != "all":
            raise CatalogError(
                f"No local catalog for {plugin.name!r} in {directory}; run harvest first"
            )
    history = (
        config.get("catalog", "version_history", False)
        if version_history is None
        else version_history
    )
    for plugin in available:
        logger.info(
            "[map] project=%s force_dataset_pid=%s",
            plugin.name,
            config.get_plugin(plugin.name, "force_dataset_pid", False),
        )
    with ExitStack() as stack:
        catalogs = {
            plugin.name: stack.enter_context(
                open_catalog(plugin.name, directory=directory, read_only=True)
            )
            for plugin in available
        }
        start, end, empty = _date_bounds(catalogs, input_date, from_date, to_date)
        if empty:
            return FeedResult()

        def records():
            for plugin in available:
                date_options = (
                    {"from_date": start, "to_date": end}
                    if start is not None or end is not None
                    else {}
                )
                with closing(catalogs[plugin.name].iter_items(**date_options)) as items:
                    for entry in items:
                        properties = entry.item.get("properties") or {}
                        if not isinstance(properties, dict):
                            properties = {}
                        publication_time = (
                            entry.publication_time
                            or properties.get("updated")
                            or properties.get("created")
                        )
                        yield (
                            f"{plugin.name}:{entry.collection_id}/{entry.item_id}",
                            {
                                "data": {
                                    "payload": {
                                        "method": "POST",
                                        "collection_id": entry.collection_id,
                                        "item": entry.item,
                                    }
                                },
                                "metadata": {
                                    "time": publication_time,
                                    "catalog_date": (
                                        entry.event_date.isoformat()
                                        if entry.event_date
                                        else None
                                    ),
                                },
                            },
                        )

        selected = stack.enter_context(
            closing(islice_records(records(), offset, limit))
        )
        # Peek only one record so empty selections still skip processor setup.
        first = next(selected, None)
        if first is None:
            return FeedResult()
        target = build_processing_target(
            projects=tuple(catalogs),
            handle_profile=handle_profile,
            catalogs=catalogs,
            version_history=history,
            require_all_assets=True,
        )
        return process_messages(
            chain((first,), selected),
            processor=target,
            verbose=verbose,
            progress=progress,
            handle_profile=handle_profile,
            max_errors=config.get("consumer", "max_errors", 1),
        )


def islice_records(records, offset, limit):
    """Close the nested item iterator even when a bounded selection stops early."""
    with closing(records):
        yield from islice(records, offset, None if limit is None else offset + limit)


def _date_bounds(
    catalogs, input_date, from_date, to_date
) -> tuple[date | None, date | None, bool]:
    if input_date == "last":
        days = [
            day
            for catalog in catalogs.values()
            if (day := catalog.latest_event_date()) is not None
        ]
        latest = max(days, default=None)
        return latest, latest, latest is None
    if isinstance(input_date, datetime):
        return input_date.date(), input_date.date(), False
    if input_date not in (None, "all"):
        raise ValueError(f"Unresolved date selector: {input_date!r}")
    return (
        from_date.date() if from_date is not None else None,
        to_date.date() if to_date is not None else None,
        False,
    )
