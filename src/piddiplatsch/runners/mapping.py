"""Stream complete local catalog items through the shared processing pipeline."""

from contextlib import ExitStack, closing
from itertools import chain, islice
from pathlib import Path

from piddiplatsch.catalog.api import (
    get_catalog_directory,
    get_catalog_path,
    open_catalog,
)
from piddiplatsch.catalog.state import HarvestState
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


def map_catalog(
    *,
    directory: Path | None = None,
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
    available registered catalogs. Order follows the project selection, then
    collection/item identity; offsets and limits count items across projects.
    """
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")
    if offset < 0:
        raise ValueError("offset cannot be negative")

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
    with ExitStack() as stack:
        catalogs = {
            plugin.name: stack.enter_context(
                open_catalog(plugin.name, directory=directory, read_only=True)
            )
            for plugin in available
        }

        def records():
            for plugin in available:
                with closing(catalogs[plugin.name].iter_items()) as items:
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
                                "metadata": {"time": publication_time},
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
        )
        return process_messages(
            chain((first,), selected),
            processor=target,
            verbose=verbose,
            progress=progress,
            handle_profile=handle_profile,
            max_errors=config.get("consumer", "max_errors", -1),
        )


def islice_records(records, offset, limit):
    """Close the nested item iterator even when a bounded selection stops early."""
    with closing(records):
        yield from islice(records, offset, None if limit is None else offset + limit)
