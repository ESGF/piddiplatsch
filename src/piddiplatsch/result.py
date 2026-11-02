from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ProcessingResult:
    key: str
    num_handles: int = 0
    success: bool = False
    error: str | None = None
    skipped: bool = False
    skip_reason: str | None = None
    filtered: bool = False
    filtered_reason: str | None = None
    project: str | None = None
    plugin: str | None = None
    patched: bool = False
    # Source event type for stored harvest events and mapped/rejected work.
    event_type: str | None = None
    elapsed: float = 0.0

    # Detailed timings
    handle_processing_time: float = 0.0

    # Durable catalog outcomes; independent of prepared Handle counts.
    catalog_written: bool = False
    catalog_refreshed: bool = False

    # Alias Handles are included in num_handles.
    num_alias_handles: int = 0


@dataclass
class FeedResult:
    """Result of feeding messages directly through the pipeline."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    filtered: int = 0


@dataclass
class ProjectPublishResult:
    """Per-project publication counters."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0


@dataclass
class PublishResult:
    """Result of publishing prepared Handle records from JSONL files."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    retry_attempts: int = 0
    errors: list[str] = field(default_factory=list)
    result_file: Path | None = None
    project: str | None = None
    projects: dict[str, ProjectPublishResult] = field(default_factory=dict)


@dataclass
class PrepareResult:
    payload: dict
    infos: dict | None = None
    subdir: Path | None = None
