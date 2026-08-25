import logging
from collections.abc import Callable
from pathlib import Path

from piddiplatsch.exceptions import JsonlReadError
from piddiplatsch.helpers import find_jsonl, read_jsonl
from piddiplatsch.result import RetryResult


def load_failed_messages(jsonl_path: Path) -> list[tuple[str, dict]]:
    """Load failed (or skipped) items from JSONL and return as (key, value) tuples."""
    records = read_jsonl(jsonl_path)
    if not records:
        logging.error(f"Retry file not found or empty: {jsonl_path}")
        return []

    messages: list[tuple[str, dict]] = []
    for record in records:
        key = str(record.get("key") or record.get("id") or "unknown")
        if "__infos__" in record:
            record["__infos__"]["retries"] = int(record["__infos__"].get("retries", 0)) + 1
        else:
            record["retries"] = int(record.get("retries", 0)) + 1
        messages.append((key, record))

    logging.info(f"Loaded {len(messages)} messages from {jsonl_path}")
    return messages


def find_retry_files(paths: tuple[Path, ...]) -> list[Path]:
    """
    Find all JSONL files from the given paths.

    Supports files, directories, and glob patterns. Returns sorted unique paths.
    """
    return find_jsonl(paths)


class RetryRunner:
    """Encapsulates retry policy and execution for processing failed items.

    Configure once per run to avoid repeating arguments across functions.

    Example:
        from pathlib import Path
        from piddiplatsch.persist.retry import RetryRunner

        runner = RetryRunner(
            "cmip6",
            failure_dir=Path("outputs/failures"),
            delete_after=False,
            dry_run=True,
        )
        # Single file
        result = runner.run_file(Path("outputs/failures/r0/failed_items.jsonl"))
        # Batch
        overall = runner.run_batch((Path("outputs/failures/r0"),))
    """

    def __init__(
        self,
        processor=None,
        *,
        projects: list[str] | tuple[str, ...] | str | None = None,
        failure_dir: Path,
        delete_after: bool = False,
        dry_run: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        if processor is not None and projects is not None:
            raise ValueError("Specify either processor or projects, not both")
        self.processor = processor
        self.projects = projects
        self.failure_dir = failure_dir
        self.delete_after = delete_after
        self.dry_run = dry_run
        self.logger = logger or logging.getLogger(__name__)

    def run_file(self, jsonl_path: Path) -> RetryResult:
        """Retry failed items from a JSONL file by reprocessing them through the pipeline."""
        from piddiplatsch.consumer import feed_messages_direct

        try:
            messages = load_failed_messages(jsonl_path)
        except JsonlReadError as exc:
            self.logger.error(str(exc))
            return RetryResult(total=1, failed=1, errors=[str(exc)])

        result = RetryResult(total=len(messages))
        if not messages:
            self.logger.warning("No messages to retry.")
            return result

        selection = self.processor if self.processor is not None else self.projects
        self.logger.info(f"Retrying {len(messages)} messages from {jsonl_path} using '{selection}'...")

        # Track failure files before retry
        failure_files_before = {path: path.stat().st_size for path in self.failure_dir.rglob("*.jsonl")}

        # Process messages through pipeline
        feed_result = feed_messages_direct(
            messages,
            processor=self.processor,
            projects=self.projects,
            dry_run=self.dry_run,
            failure_dir=self.failure_dir,
            force=True,
        )

        # Find new failure files created during retry
        failure_files_after = set(self.failure_dir.rglob("*.jsonl"))
        result.failure_files = {
            path for path in failure_files_after if path not in failure_files_before or path.stat().st_size != failure_files_before[path]
        }

        # Use stats from feed_result
        result.succeeded = feed_result.succeeded
        result.skipped = feed_result.skipped
        result.filtered = feed_result.filtered
        # A filtered retry was not handled by a selected plugin. Keep the
        # original input instead of treating it as successfully recovered.
        result.failed = feed_result.failed + feed_result.skipped + feed_result.filtered

        if self.delete_after and result.failed == 0:
            try:
                jsonl_path.unlink()
                self.logger.info(f"Deleted retry file: {jsonl_path}")
            except Exception as e:
                self.logger.warning(f"Could not delete {jsonl_path}: {e}")
        elif self.delete_after and result.failed > 0:
            self.logger.info(f"Skipping deletion of {jsonl_path} because {result.failed} items failed again")

        return result

    def run_batch(
        self,
        paths: tuple[Path, ...],
        *,
        verbose: bool = False,
        progress_callback: Callable[[Path, int, int, RetryResult], None] | None = None,
    ) -> RetryResult:
        """Retry failed items from multiple files/directories and aggregate results."""
        files = find_retry_files(paths)

        if not files:
            self.logger.warning("No retry files found.")
            return RetryResult()

        self.logger.info(f"Found {len(files)} file(s) to retry.")

        overall = RetryResult()
        total_files = len(files)

        for idx, file in enumerate(files, 1):
            result = self.run_file(file)
            overall.total += result.total
            overall.succeeded += result.succeeded
            overall.failed += result.failed
            overall.skipped += result.skipped
            overall.filtered += result.filtered
            overall.failure_files.update(result.failure_files)
            overall.errors.extend(result.errors)

            if progress_callback:
                progress_callback(file, idx, total_files, result)

            if verbose:
                self.logger.info(f"[{idx}/{total_files}] {file.name}: total={result.total}, succeeded={result.succeeded}, failed={result.failed}")

        return overall
