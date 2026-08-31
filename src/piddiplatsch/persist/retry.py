import logging
from collections.abc import Callable
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.exceptions import JsonlReadError
from piddiplatsch.helpers import find_jsonl, read_jsonl, utc_now
from piddiplatsch.result import FeedResult, RetryResult


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
            record["__infos__"]["retries"] = (
                int(record["__infos__"].get("retries", 0)) + 1
            )
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
            projects=["cmip6"],
            failure_dir=Path("outputs/failures"),  # legacy/unresolved records
            delete_after=False,
            publish=False,
        )
        # Single file
        result = runner.run_file(
            Path("outputs/cmip6/failures/r0/failed_items.jsonl")
        )
        # Batch
        overall = runner.run_batch((Path("outputs/cmip6/failures/r0"),))
    """

    def __init__(
        self,
        *,
        projects: list[str] | tuple[str, ...] | str,
        failure_dir: Path,
        delete_after: bool = False,
        publish: bool = False,
        handle_profile: str | None = None,
        handle_output_filename: str | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.projects = projects
        self.failure_dir = failure_dir
        self.delete_after = delete_after
        self.publish = publish
        self.handle_profile = handle_profile
        self.handle_output_filename = handle_output_filename or (
            f"retry_handles_{utc_now():%Y-%m-%d_%H-%M-%S-%f}.jsonl"
        )
        self.output_dir = Path(config.get("consumer", {}).get("output_dir", "outputs"))
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

        self.logger.info(
            f"Retrying {len(messages)} messages from {jsonl_path} using '{self.projects}'..."
        )

        # Track failure files before retry
        failure_files_before = {
            path: path.stat().st_size for path in self._failure_files()
        }

        # Records with persisted provenance are retried through exactly that
        # plugin. Legacy records without it retain the configured selection.
        feed_result = FeedResult()
        for project, project_messages in self._group_messages(messages).items():
            selection = [project] if project else self.projects
            partial = feed_messages_direct(
                project_messages,
                projects=selection,
                publish=self.publish,
                handle_profile=self.handle_profile,
                handle_output_filename=self.handle_output_filename,
                force=True,
            )
            feed_result.total += partial.total
            feed_result.succeeded += partial.succeeded
            feed_result.failed += partial.failed
            feed_result.skipped += partial.skipped
            feed_result.filtered += partial.filtered

        # Find new failure files created during retry
        failure_files_after = self._failure_files()
        result.failure_files = {
            path
            for path in failure_files_after
            if path not in failure_files_before
            or path.stat().st_size != failure_files_before[path]
        }
        result.handle_files = set(
            self.output_dir.glob(f"*/handles/{self.handle_output_filename}")
        )

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
            self.logger.info(
                f"Skipping deletion of {jsonl_path} because {result.failed} items failed again"
            )

        return result

    def _failure_files(self) -> set[Path]:
        files = set(self.failure_dir.rglob("*.jsonl"))
        files.update(self.output_dir.rglob("failed_items_*.jsonl"))
        return files

    @staticmethod
    def _group_messages(
        messages: list[tuple[str, dict]],
    ) -> dict[str | None, list[tuple[str, dict]]]:
        groups: dict[str | None, list[tuple[str, dict]]] = {}
        for key, record in messages:
            infos = record.get("__infos__", {}) or {}
            project = infos.get("project")
            if not isinstance(project, str) or not project.strip():
                project = None
            groups.setdefault(project, []).append((key, record))
        return groups

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
            overall.handle_files.update(result.handle_files)
            overall.errors.extend(result.errors)

            if progress_callback:
                progress_callback(file, idx, total_files, result)

            if verbose:
                self.logger.info(
                    f"[{idx}/{total_files}] {file.name}: total={result.total}, succeeded={result.succeeded}, failed={result.failed}"
                )

        return overall
