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
    transient_skip: bool = False
    filtered: bool = False
    filtered_reason: str | None = None
    project: str | None = None
    plugin: str | None = None
    patched: bool = False
    elapsed: float = 0.0

    # Detailed timings
    handle_processing_time: float = 0.0


@dataclass
class FeedResult:
    """Result of feeding messages directly through the pipeline."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    filtered: int = 0


@dataclass
class RetryResult:
    """Result of retrying failed items."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    filtered: int = 0
    failure_files: set[Path] = field(default_factory=set)
    errors: list[str] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Calculate success rate as a percentage."""
        return (self.succeeded / self.total * 100) if self.total > 0 else 0.0


@dataclass
class PublishResult:
    """Result of publishing prepared Handle records from JSONL files."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    retry_attempts: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class PrepareResult:
    payload: dict
    infos: dict | None = None
    subdir: Path | None = None
