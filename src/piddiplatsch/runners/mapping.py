"""Stream raw JSONL records through the shared processing pipeline."""

from contextlib import closing
from itertools import chain
from pathlib import Path

from piddiplatsch.core.pipeline import build_processing_target, process_messages
from piddiplatsch.helpers import find_jsonl
from piddiplatsch.jsonl_stream import iter_jsonl_records
from piddiplatsch.monitoring.progress import BaseProgress
from piddiplatsch.result import FeedResult


def map_dump_files(
    paths: list[Path] | tuple[Path, ...],
    *,
    projects: list[str] | tuple[str, ...] | str | None = None,
    limit: int | None = None,
    offset: int = 0,
    force: bool = False,
    verbose: bool = False,
    progress: BaseProgress | None = None,
    handle_profile: str | None = None,
) -> FeedResult:
    """Map saved raw-message JSONL through project plugins without Kafka."""
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")
    if offset < 0:
        raise ValueError("offset cannot be negative")

    with closing(
        iter_jsonl_records(find_jsonl(paths), offset=offset, limit=limit)
    ) as records:
        # Peek only one record so empty selections still skip processor setup.
        first = next(records, None)
        if first is None:
            return FeedResult()
        target = build_processing_target(
            projects=projects,
            handle_profile=handle_profile,
        )
        if not force:
            target.preflight_check(stop_on_transient_skip=True)
        messages = (
            (f"{path}:{line_number}", record)
            for path, line_number, record in chain((first,), records)
        )
        return process_messages(
            messages,
            processor=target,
            force=force,
            verbose=verbose,
            progress=progress,
            handle_profile=handle_profile,
        )
