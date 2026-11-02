"""Map pending work into the SQLite Handle outbox without publishing."""

import json
from datetime import datetime
from pathlib import Path

from piddiplatsch.commands.helper import validate_date_selection
from piddiplatsch.core.pipeline import configured_projects
from piddiplatsch.core.registry import get_plugins
from piddiplatsch.exceptions import BatchError
from piddiplatsch.result import FeedResult
from piddiplatsch.runners.sync import SyncWorker


def map_catalog(
    *,
    directory: Path | None = None,
    input_date=None,
    from_date=None,
    to_date=None,
    projects=None,
    limit=None,
    offset=0,
    verbose=False,
    progress=None,
    handle_profile=None,
    version_history=None,
):
    """Map pending event snapshots; selectors count work entries across projects.

    Date filters select local work creation days in UTC. No date selector means
    all pending work. Output and database progress use the same sealing/recovery
    path as sync, under the same worker lock.
    """
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")
    if offset < 0:
        raise ValueError("offset cannot be negative")
    validate_date_selection(input_date, from_date, to_date)
    selected = get_plugins(configured_projects() if projects is None else projects)
    worker = SyncWorker(
        projects=tuple(plugin.name for plugin in selected),
        progress=progress,
        directory=directory,
        handle_profile=handle_profile,
        version_history=version_history,
        require_configured=False,
    )
    if input_date == "last":
        from piddiplatsch.catalog.api import get_catalog_path, open_catalog

        days = []
        for plugin in selected:
            if get_catalog_path(plugin.name, directory=directory).exists():
                with open_catalog(
                    plugin.name, directory=directory, read_only=True
                ) as catalog:
                    row = catalog._execute(
                        "SELECT MAX(created_at) FROM work_items WHERE project=? AND state='pending'",
                        (plugin.name,),
                    ).fetchone()[0]
                    if row:
                        days.append(datetime.fromisoformat(row).date())
        if not days:
            return FeedResult()
        worker.from_date = worker.to_date = max(days)
    elif isinstance(input_date, datetime):
        worker.from_date = worker.to_date = input_date.date()
    elif input_date not in (None, "all"):
        raise ValueError(f"Unresolved date selector: {input_date!r}")
    else:
        worker.from_date = from_date.date() if from_date else None
        worker.to_date = to_date.date() if to_date else None
    worker.remaining = limit
    worker.offset = offset
    paths = worker.run(force_map=True, map_only=True)
    result = FeedResult()
    for path in paths:
        manifest = json.loads((path / "manifest.json").read_text())
        result.total += manifest["item_count"]
        result.succeeded += manifest["succeeded_count"]
        result.failed += manifest["failed_count"]
    if worker.errors and not result.failed:
        raise BatchError("; ".join(worker.errors))
    return result
